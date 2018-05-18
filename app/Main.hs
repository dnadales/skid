{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Entry point for 'skid'.
module Main where

import           Control.Concurrent.Async (wait)
import           Control.Monad            (unless, when)
import           System.Console.Docopt    (Arguments, Docopt, docopt,
                                           exitWithUsage, getArgWithDefault,
                                           isPresent, longOption,
                                           parseArgsOrExit)
import           System.Environment       (getArgs)
import           Text.Read                (readEither)

import           Database.SKID.Node       (runNode)
import           Database.SKID.Node.State (mkState)
import           Database.SKID.REPL       (runREPL)

patterns :: Docopt
patterns = [docopt|
skid

Usage:
  skid [options]

Options:
-p --port <number>    Port number in which the node should be started. If not
                      present the default port 9090 will be chosen. Port numbers
                      must be in the range 1024-65535

-i --interactive      Start a simple REPL for interacting with the node and
                      storing and retrieving key-value pairs,
|]

defaultPort :: String
defaultPort = "9090"

defaultHost :: String
defaultHost = "localhost"

minPortValue :: Int
minPortValue = 1024

maxPortValue :: Int
maxPortValue = 65535

main :: IO ()
main = do
    args  <- parseArgsOrExit patterns =<< getArgs
    port  <- getPort args
    st    <- mkState
    aNode <- runNode defaultHost port st
    when (args `isPresent` longOption "interactive") (runREPL st)
    -- When the REPL exits, the node will continue running.
    wait aNode
    where
      getPort :: Arguments -> IO String
      getPort args = do
          let portStr = getArgWithDefault args defaultPort (longOption "port")
          val <- case readEither portStr of
              Left _ -> do
                  putStrLn $ "Port number should be an integer. Got " ++ portStr
                  exitWithUsage patterns
              Right (val :: Int) ->
                  return val
          unless (minPortValue <= val && val <= maxPortValue) $ do
              putStrLn $ "Invalid port number: " ++ show val
                       ++ ", a port number should be in the range "
                       ++ show minPortValue ++ " - " ++ show maxPortValue
              exitWithUsage patterns
          return portStr
