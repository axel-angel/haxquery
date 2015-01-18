{-# LANGUAGE TupleSections, FlexibleContexts, OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
import Data.Generics.Uniplate.Data
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
conv = descendBiM convTRef


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
    vexprs' <- forM vexprs $ descendBiM convTRef
    return $ TRFunction names vexprs'
convTRef (TRLateral tref) = TRLateral <$> convTRef tref


convName :: MonadState TableMap m => Name -> m Name
convName sqlName = do
    let name = case sqlName of
            Name s -> s
            QName s -> s
            UQName s -> s
    n <- registerTable name
    return $ Name n


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
