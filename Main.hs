import System.Environment
import Language.SQL.SimpleSQL.Parser
import Language.SQL.SimpleSQL.Pretty

parseSql = parseQueryExpr "" Nothing

main :: IO ()
main = do
    args <- getArgs
    let esql = parseSql $ args !! 0
    case esql of
         Left e -> putStrLn $ "Error: " ++ show e
         Right sql -> putStrLn $ prettyQueryExpr sql
