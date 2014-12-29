{-# LANGUAGE TupleSections, FlexibleContexts #-}
import Control.Applicative
import Control.Arrow
import Control.Monad.State
import System.Environment
import Language.SQL.SimpleSQL.Syntax
import Language.SQL.SimpleSQL.Parser (parseQueryExpr)
import Language.SQL.SimpleSQL.Pretty (prettyQueryExpr)
import qualified Database.SQLite.Simple as SQL
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
         Left e -> putStrLn $ "Error: " ++ show e
         Right sql -> SQL.withConnection ":memory:" $ runSql sql


runSql :: QueryExpr -> SQL.Connection -> IO ()
runSql sql conn = do
    let tstate = prettyQueryExpr <$> conv sql
    let (expr, tmap) = runState tstate emptyTableMap

    loadFiles conn tmap

    putStrLn $ "State: " ++ show tmap
    putStrLn $ "SQL: " ++ expr

    rs <- SQL.query_ conn (SQL.Query $ pack expr) :: IO [[Text]]
    forM_ rs (putStrLn . show)


{-- Parse files to fill content-equivalent tables --}

loadFiles :: SQL.Connection -> TableMap -> IO ()
loadFiles conn tmap = forM_ (stateTs tmap) $ \(fpath, tname) -> do
    -- create the table
    createTable conn tname
    -- open file, read line by line
    content <- readFile fpath
    -- split using regex
    -- TODO: detect from extension: csv, etc
    foldM_ (fillLines conn tname) 0 $ lines content


fillLines :: SQL.Connection -> String -> Int -> String -> IO Int
fillLines conn tname cols line = do
    let fields = splitRegex rxSplit line
    -- add more columns when needed
    forM_ [cols..length fields - 1] $ \i -> do
        addColumn conn tname ("c" ++ show i)
    -- fill the table
    let vstr = intercalate "," $ ("NULL" : map (const "?") fields)
    let q = "INSERT INTO "++ tname ++" VALUES ("++ vstr ++")"
    SQL.execute conn (SQL.Query $ pack q) $ fields
    -- keep track of the number of columns so far
    return $ length fields


rxSplit :: Regex
rxSplit = mkRegex "\t|  +"


createTable :: SQL.Connection -> String -> IO ()
createTable conn tname = do
    let q = "CREATE TABLE "++ tname ++" (id INTEGER PRIMARY KEY)"
    SQL.execute_ conn (SQL.Query $ pack q)

addColumn :: SQL.Connection -> String -> String -> IO ()
addColumn conn tname cname = do
    let q = "ALTER TABLE "++ tname ++" ADD COLUMN "++ cname ++" STRING"
    SQL.execute_ conn (SQL.Query $ pack q)

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

parseSql :: String -> Either String QueryExpr
parseSql = left show . parseQueryExpr "" Nothing
