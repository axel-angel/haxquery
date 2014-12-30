{-# LANGUAGE TupleSections, FlexibleContexts, OverloadedStrings #-}
import Control.Applicative
import Control.Arrow
import Control.Monad.State
import System.Environment
import Language.SQL.SimpleSQL.Syntax
import Language.SQL.SimpleSQL.Parser (parseQueryExpr, ParseError(..))
import Language.SQL.SimpleSQL.Pretty (prettyQueryExpr)
import qualified Database.SQLite3 as SQL
import Text.Regex (mkRegex, splitRegex, Regex)
import Data.Text (pack, unpack, Text)
import Data.List (intercalate)

data TableMap = TableMap { stateTs :: [(FilePath,String)], stateI :: Int }
    deriving (Show)

emptyTableMap :: TableMap
emptyTableMap = TableMap [] 0


main :: IO ()
main = do
    args <- getArgs
    let esql = parseSql (args !! 0)
    case esql of
         Left (ParseError _ _ _ err) -> do
             putStrLn "Error: Cannot parse SQL query"
             putStrLn err
         Right sql -> do
             conn <- SQL.open ":memory:"
             runQuery sql conn
             SQL.close conn


runQuery :: QueryExpr -> SQL.Database -> IO ()
runQuery sql conn = do
    let tstate = prettyQueryExpr <$> conv sql
    let (expr, tmap) = runState tstate emptyTableMap

    loadFiles conn tmap

    putStrLn $ "State: " ++ show tmap
    putStrLn $ "SQL: " ++ expr

    SQL.execWithCallback conn (pack expr) $ \_cols _cns cs -> do
        putStrLn . intercalate "\t" $ map (unpack . textMaybe) cs


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
    -- TODO: detect from extension: csv, etc
    -- fill the table (has already one column!)
    foldM_ (fillLines conn tname) 1 $ lines content


fillLines :: SQL.Database -> String -> Int -> String -> IO Int
fillLines conn tname cols line = do
    -- split using regex
    let fields = splitRegex rxSplit line
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
    return $ length fields


rxSplit :: Regex
rxSplit = mkRegex "\t|  +"


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
    qeFrom' <- forM (qeFrom sql) convTRef
    return sql { qeFrom = qeFrom' }

conv sql@(CombineQueryExpr _ _ _ _ _) = do
    qe0' <- conv $ qe0 sql
    qe1' <- conv $ qe1 sql
    return sql { qe0 = qe0', qe1 = qe1' }

conv sql@(With _ _ _) = do
    qeViews' <- forM (qeViews sql) $ \(al, qe) -> (al,) <$> conv qe
    qeQueryExpression' <- conv $ qeQueryExpression sql
    return sql { qeViews = qeViews', qeQueryExpression = qeQueryExpression' }

conv sql@(Values _) = return sql

conv sql@(Table _) = return sql


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

convVExpr :: Monad m => a -> m a
convVExpr vexpr = return vexpr


registerTable :: MonadState TableMap m => FilePath -> m String
registerTable fpath = do
    TableMap ts i <- get
    case lookup fpath ts of
         Just tname ->
             return tname
         Nothing -> do
            let tname = "t" ++ show i
            put $ TableMap ((fpath, tname):ts) (i+1)
            return tname


{-- Others/utilities --}

parseSql :: String -> Either ParseError QueryExpr
parseSql = parseQueryExpr "" Nothing
