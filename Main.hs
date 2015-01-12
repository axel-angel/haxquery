{-# LANGUAGE TupleSections, FlexibleContexts, OverloadedStrings #-}
import Control.Applicative
import Control.Monad.State
import Control.Arrow
import Language.SQL.SimpleSQL.Syntax
import Language.SQL.SimpleSQL.Parser (parseQueryExpr, ParseError(..))
import Language.SQL.SimpleSQL.Pretty (prettyQueryExpr)
import qualified Database.SQLite3 as SQL
import Text.Regex (Regex, mkRegex, splitRegex, matchRegex)
import Data.Text (pack, unpack, Text)
import Data.List (intercalate)
import System.Exit (exitWith, ExitCode(..))
import Text.ParserCombinators.Parsec (parse)
import Data.CSV (csvFile, genCsvFile)
import Options.Applicative hiding (ParseError)
import qualified Data.Traversable as T (sequence)

data TableMap = TableMap { stateTs :: [(FilePath,String)], _stateI :: Int }
    deriving (Show)

emptyTableMap :: TableMap
emptyTableMap = TableMap [] 0

data Args = Args { argQuery :: QueryExpr, argOut :: Format }
    deriving (Show)

data Format = TSV | CSV
    deriving (Show, Enum)
instance Read Format where
    readsPrec _ "csv" = [(CSV,"")]
    readsPrec _ "tsv" = [(TSV,"")]
    readsPrec _ _ = []


main :: IO ()
main = execParser argParser >>= \args -> do
    conn <- SQL.open ":memory:"
    runQuery args conn
    SQL.close conn

argParser :: ParserInfo Args
argParser = info (helper <*> opts)
    ( fullDesc
    <> progDesc "Execute an SQL query given file-tables"
    <> header "haxquery" )


opts :: Parser Args
opts = Args
    <$> argument (eitherReader parseSql)
        ( help "SQL query to execute" )
    <*> option auto
        ( long "output"
        <> help "Format used for printing the result"
        <> value TSV )


runQuery :: Args -> SQL.Database -> IO ()
runQuery (Args sql output) conn = do
    let tstate = prettyQueryExpr <$> conv sql
    let (expr, tmap) = runState tstate emptyTableMap

    loadFiles conn tmap

    let writer :: [String] -> IO ()
        writer = case output of
            TSV -> putStrLn . intercalate "\t"
            CSV -> putStr . genCsvFile . (:[])

    SQL.execWithCallback conn (pack expr) $ \_cols _cns cs -> do
        writer . map (unpack . textMaybe) $ cs


textMaybe :: Maybe Text -> Text
textMaybe (Just x) = x
textMaybe Nothing = ""


{-- Parse files to fill content-equivalent tables --}

loadFiles :: SQL.Database -> TableMap -> IO ()
loadFiles conn tmap = forM_ (stateTs tmap) $ \(fpath, tname) -> do
    -- create the table
    createTable conn tname
    -- open file, read line by line
    content <- readFile fpath
    -- detect from extension: csv, etc
    let reader :: String -> Either String [[String]]
        reader = case matchRegex (mkRegex "\\.csv") fpath of
                      Nothing -> Right . map (splitRegex rxSplit) . lines
                      Just _ -> csvParse fpath
    -- fill the table (has already one column!)
    case reader content of
         Right rows -> foldM_ (fillLines conn tname) 1 rows
         Left err -> do
             putStrLn $ "Error parsing: "++ err
             exitWith $ ExitFailure 1


rxSplit :: Regex
rxSplit = mkRegex "\t|  +"


csvParse :: FilePath -> String -> Either String [[String]]
csvParse fpath content = left (show) $ parse csvFile fpath content


fillLines :: SQL.Database -> String -> Int -> [String] -> IO Int
fillLines conn tname cols fields = do
    -- add more columns when needed
    forM_ [cols..length fields - 1] $ \i -> do
        addColumn conn tname ("c" ++ show i)
    -- fill the table
    let colsns = [ "c" ++ show i | i <- [0..length fields - 1] ]
    let colsstr = intercalate "," colsns
    let vstr = intercalate "," $ map (const "?") fields
    let q = "INSERT INTO "++ tname ++" ("++ colsstr ++") VALUES ("++ vstr ++")"
    execBind conn (pack q) $ map (SQL.SQLText . pack) fields
    -- keep track of the number of columns so far
    return $ max cols (length fields)


createTable :: SQL.Database -> String -> IO ()
createTable conn tname = do
    -- FIXME: suppose one column at least
    let q = "CREATE TABLE "++ tname ++" (c0 string)"
    SQL.exec conn $ pack q

addColumn :: SQL.Database -> String -> String -> IO ()
addColumn conn tname cname = do
    let q = "ALTER TABLE "++ tname ++" ADD COLUMN "++ cname ++" STRING"
    SQL.exec conn $ pack q

execBind :: SQL.Database -> Text -> [SQL.SQLData] -> IO ()
execBind conn query binds = do
    sta <- SQL.prepare conn query
    SQL.bind sta binds
    _ <- SQL.step sta
    SQL.finalize sta
    return ()


{-- Find and register file-table in query --}

conv :: QueryExpr -> State TableMap QueryExpr
conv sql@(Select _ _ _ _ _ _ _ _ _)  = do
    qeSelectList' <- forM (qeSelectList sql) $
        \(vexpr, mname) -> (,mname) <$> convVExpr vexpr
    qeFrom' <- forM (qeFrom sql) convTRef
    qeWhere' <- T.sequence $ convVExpr <$> qeWhere sql
    qeGroupBy' <- forM (qeGroupBy sql) convGExpr
    qeHaving' <- T.sequence $ convVExpr <$> qeHaving sql
    qeOrderBy' <- forM (qeOrderBy sql) convSortSpec
    qeOffset' <- T.sequence $ convVExpr <$> qeOffset sql
    qeFetchFirst' <- T.sequence $ convVExpr <$> qeFetchFirst sql
    return $ sql
        { qeSelectList = qeSelectList'
        , qeFrom = qeFrom'
        , qeWhere = qeWhere'
        , qeGroupBy = qeGroupBy'
        , qeHaving = qeHaving'
        , qeOrderBy = qeOrderBy'
        , qeOffset = qeOffset'
        , qeFetchFirst = qeFetchFirst' }

conv sql@(CombineQueryExpr _ _ _ _ _) = do
    qe0' <- conv $ qe0 sql
    qe1' <- conv $ qe1 sql
    return sql { qe0 = qe0', qe1 = qe1' }

conv (With wr vs qe) = do
    vs' <- forM vs $ \(al, qer) -> (al,) <$> conv qer
    qe' <- conv qe
    return $ With wr vs' qe'

conv (Values vss) = Values <$> (forM vss $ \vs -> forM vs convVExpr)

conv (Table ns) = Table <$> forM ns convName


convTRef :: TableRef -> State TableMap TableRef
convTRef (TRSimple names) = TRSimple <$> forM names convName
convTRef (TRJoin tref1 nat jtype tref2 mJoinCond) = do
    tref1' <- convTRef tref1
    tref2' <- convTRef tref2
    return $ TRJoin tref1' nat jtype tref2' mJoinCond
convTRef (TRParens tref) = TRParens <$> convTRef tref
convTRef (TRAlias tref alias) = do
    tref' <- convTRef tref
    return $ TRAlias tref' alias
convTRef (TRQueryExpr expr) = TRQueryExpr <$> conv expr
convTRef (TRFunction names vexprs) = do
    names' <- forM names convName
    vexprs' <- forM vexprs convVExpr
    return $ TRFunction names' vexprs'
convTRef (TRLateral tref) = TRLateral <$> convTRef tref


convName :: MonadState TableMap m => Name -> m Name
convName sqlName = do
    let name = case sqlName of
            Name s -> s
            QName s -> s
            UQName s -> s
    n <- registerTable name
    return $ Name n


convVExpr :: ValueExpr -> State TableMap ValueExpr
convVExpr (App ns vs) = App ns <$> forM vs convVExpr
convVExpr agg@(AggregateApp _ _ _ _ _) = do
    aggArgs' <- forM (aggArgs agg) convVExpr
    aggFilter' <- T.sequence $ convVExpr <$> aggFilter agg
    return $ agg { aggArgs = aggArgs', aggFilter = aggFilter' }
convVExpr agg@(AggregateAppGroup _ _ _) = do
    aggArgs' <- forM (aggArgs agg) convVExpr
    return $ agg { aggArgs = aggArgs' }
convVExpr win@(WindowApp _ _ _ _ _) = do
    wnArgs' <- forM (wnArgs win) convVExpr
    wnPartition' <- forM (wnPartition win) convVExpr
    return $ win { wnArgs = wnArgs', wnPartition = wnPartition' }
convVExpr (BinOp vexprl ns vexprr) = do
    vexprl' <- convVExpr vexprl
    vexprr' <- convVExpr vexprr
    return $ BinOp vexprl' ns vexprr'
convVExpr (PrefixOp ns vexpr) = PrefixOp ns <$> convVExpr vexpr
convVExpr (PostfixOp ns vexpr) = PostfixOp ns <$> convVExpr vexpr
convVExpr (SpecialOp ns vexprs) = SpecialOp ns <$> forM vexprs convVExpr
convVExpr (SpecialOpK ns mvexpr xs) = do
    mvexpr' <- T.sequence $ convVExpr <$> mvexpr
    xs' <- forM xs $ \(s, vexpr) -> (s,) <$> convVExpr vexpr
    return $ SpecialOpK ns mvexpr' xs'
convVExpr (Case ct cw ce) = do
    ct' <- T.sequence $ convVExpr <$> ct
    cw' <- forM cw $ \(vs, vexpr) -> do
        vs' <- forM vs convVExpr
        vexpr' <- convVExpr vexpr
        return (vs', vexpr')
    ce' <- T.sequence $ convVExpr <$> ce
    return $ Case ct' cw' ce'
convVExpr (Parens vexpr) = Parens <$> convVExpr vexpr
convVExpr (Cast vexpr tn) = flip Cast tn <$> convVExpr vexpr
convVExpr (SubQueryExpr tp qexpr) = SubQueryExpr tp <$> conv qexpr
convVExpr (In b vexpr inp) = do
    vexpr' <- convVExpr vexpr
    inp' <- convInPredValue inp
    return $ In b vexpr' inp'
convVExpr (QuantifiedComparison vexpr ns cpq qexpr) = do
    vexpr' <- convVExpr vexpr
    qexpr' <- conv qexpr
    return $ QuantifiedComparison vexpr' ns cpq qexpr'
convVExpr (Match vexpr b qexpr) = do
    vexpr' <- convVExpr vexpr
    qexpr' <- conv qexpr
    return $ Match vexpr' b qexpr'
convVExpr (Array vexpr vs) = do
    vexpr' <- convVExpr vexpr
    vs' <- forM vs convVExpr
    return $ Array vexpr' vs'
convVExpr (ArrayCtor qexpr) = ArrayCtor <$> conv qexpr
convVExpr (Escape vexpr c) = flip Escape c <$> convVExpr vexpr
convVExpr (UEscape vexpr c) = flip UEscape c <$> convVExpr vexpr
convVExpr (Collate vexpr ns) = flip Collate ns <$> convVExpr vexpr
convVExpr (MultisetBinOp vexprl co sq vexprr) = do
    vexprl' <- convVExpr vexprl
    vexprr' <- convVExpr vexprr
    return $ MultisetBinOp vexprl' co sq vexprr'
convVExpr (MultisetCtor vs) = MultisetCtor <$> forM vs convVExpr
convVExpr (MultisetQueryCtor qs) = MultisetQueryCtor <$> conv qs
convVExpr vexpr = return vexpr


convGExpr :: GroupingExpr -> State TableMap GroupingExpr
convGExpr (GroupingParens gs) = GroupingParens <$> forM gs convGExpr
convGExpr (Cube gs) = Cube <$> forM gs convGExpr
convGExpr (Rollup gs) = Rollup <$> forM gs convGExpr
convGExpr (GroupingSets gs) = GroupingSets <$> forM gs convGExpr
convGExpr (SimpleGroup vexpr) = SimpleGroup <$> convVExpr vexpr


convSortSpec :: SortSpec -> State TableMap SortSpec
convSortSpec (SortSpec vexpr dir no) = do
    vexpr' <- convVExpr vexpr
    return $ SortSpec vexpr' dir no


convInPredValue :: InPredValue -> State TableMap InPredValue
convInPredValue (InList vs) = InList <$> forM vs convVExpr
convInPredValue (InQueryExpr qexpr) = InQueryExpr <$> conv qexpr

registerTable :: MonadState TableMap m => FilePath -> m String
registerTable fpath' = do
    let fpath = if fpath' /= "-" then fpath' else "/dev/stdin"
    TableMap ts i <- get
    case lookup fpath ts of
         Just tname ->
             return tname
         Nothing -> do
            let tname = "t" ++ show i
            put $ TableMap ((fpath, tname):ts) (i+1)
            return tname


{-- Others/utilities --}

parseSql :: String -> Either String QueryExpr
parseSql = left showErr . parseQueryExpr "" Nothing
    where showErr (ParseError _ _ _ err) = show err
