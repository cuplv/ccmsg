{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Types
  ( NodeId (..)
  , nodeId
  , nodeIdSize
  , NodeMap
  , SeqNum
  , Debugger
  , mkPrinterDbg
  , mkNoDebugDbg
  , runQD
  , runQD'
  , debug
  ) where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import Data.IP (IP)
import Data.Map (Map)
import Data.Store
import Data.Store.TH
import Data.Word (Word32)
import Network.Socket (PortNumber,SockAddr)

type SeqNum = Word32

data NodeId = NodeId { nodeIdWord :: Word32 } deriving (Eq,Ord)

instance Show NodeId where
  show (NodeId n) = "N(" ++ show n ++ ")"

makeStore ''NodeId

nodeIdSize :: Int
nodeIdSize = case (size :: Size NodeId) of
  VarSize _ -> error "NodeId has var size for Store representation"
  ConstSize i -> i

nodeId = NodeId

data Debugger
  = Debugger (TQueue String)
  | Printer
  | NoDebug

debug :: Debugger -> String -> IO ()
debug d s = case d of
  Debugger chan -> atomically $ writeTQueue chan s
  Printer -> putStrLn s
  NoDebug -> return ()

mkPrinterDbg = Printer

mkNoDebugDbg = NoDebug

runQD :: IO (Debugger)
runQD = do
  q <- newTQueueIO
  let printLoop = do
        msg <- atomically $ readTQueue q
        putStrLn $ "[Debug] " ++ msg
        printLoop
  forkIO $ printLoop
  return $ Debugger q

{-| Return a Queue Debugger, and the looping printer action that will
  print the debug messages to the console. -}
runQD' :: IO (Debugger, IO ())
runQD' = do
  q <- newTQueueIO
  let printLoop = do
        msg <- atomically $ readTQueue q
        putStrLn $ "[Debug] " ++ msg
        printLoop
  return (Debugger q, printLoop)

type NodeMap = Map NodeId (String,String)
