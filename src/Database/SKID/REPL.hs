-- | Read-eval-print loop for 'skid'.

module Database.SKID.REPL
    (runREPL)
where

import           Control.Monad            (when)
import           Data.Foldable            (traverse_)
import qualified Data.Set                 as Set
import           System.Console.Haskeline (InputT, autoAddHistory,
                                           defaultSettings, getInputLine,
                                           historyFile, outputStrLn, runInputT)
import           System.Directory         (getHomeDirectory)
import           System.FilePath          ((</>))

import           Database.SKID.Node.State (State, getPeers)

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
          outputStrLn "TODO: implement 'get'"
          return True
      dispatch ["put", k, v] = do
          outputStrLn "TODO: implement 'put'"
          return True
      dispatch x = do
          outputStrLn $ "Unknown command: " ++ show x
          return True
