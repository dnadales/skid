-- | Read-eval-print loop for 'skid'.

module Database.SKID.REPL
    (runREPL)
where

import           Control.Monad            (when)
import           Data.ByteString          (ByteString)
import           Data.Foldable            (traverse_)
import qualified Data.Set                 as Set
import qualified Data.Text                as T
import           Data.Text.Encoding       (encodeUtf8)
import           System.Console.Haskeline (InputT, autoAddHistory,
                                           defaultSettings, getInputLine,
                                           historyFile, outputStrLn, runInputT)
import           System.Directory         (getHomeDirectory)
import           System.FilePath          ((</>))

import           Database.SKID.Node.State (State, get, getPeers, put)

runREPL :: State -> IO ()
runREPL st = do
    home <- getHomeDirectory
    runInputT (haskelineSettings home) loop
    where
      haskelineSettings home = defaultSettings
        { historyFile = Just $ home </> ".skid-hist.txt"
        , autoAddHistory = True
        }

      loop :: InputT IO ()
      loop = do
          line <- getInputLine "> "
          cont <- maybe (return True) (dispatch . words) line
          when cont loop

      -- | Dispatch a command based on the user input and return whether
      -- we should continue with the main loop.
      dispatch :: [String] -> InputT IO Bool
      dispatch [] = return True
      dispatch ["q"] = return False
      dispatch ["quit"] = return False
      dispatch ["peers"] = do
          ns <- getPeers st
          traverse_ (outputStrLn . show) (Set.toList ns)
          return True
      dispatch ["get", k] = do
          mv <- get st (encode k)
          maybe (outputStrLn "Key not found") (outputStrLn . show) mv
          return True
      dispatch ["put", k, v] = do
          put st (encode k) (encode v)
          return True
      dispatch x = do
          outputStrLn $ "Unknown command: " ++ show x
          return True

      encode :: String -> ByteString
      encode = encodeUtf8 . T.pack
