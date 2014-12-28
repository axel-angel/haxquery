{-# LANGUAGE TupleSections, FlexibleContexts #-}
import Control.Applicative
import Control.Arrow
import Control.Monad.State
import System.Environment
import Language.SQL.SimpleSQL.Syntax
import Language.SQL.SimpleSQL.Parser (parseQueryExpr)
import Language.SQL.SimpleSQL.Pretty (prettyQueryExpr)

data TableMap = TableMap { stateTs :: [(FilePath,String)], stateI :: Int }
    deriving (Show)

emptyTableMap :: TableMap
emptyTableMap = TableMap [] 0

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

main :: IO ()
main = do
    args <- getArgs
    let esql = parseSql (args !! 0)
    case esql of
         Left e -> putStrLn $ "Error: " ++ show e
         Right sql -> do
             let tstate = prettyQueryExpr <$> conv sql
             let (expr, tmap) = runState tstate emptyTableMap
             putStrLn $ "State: " ++ show tmap
             putStrLn $ "SQL: " ++ show expr

parseSql :: String -> Either String QueryExpr
parseSql = left show . parseQueryExpr "" Nothing
