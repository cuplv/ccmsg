module Network.Ccm.TCP where

import Network.Ccm.Bsm.Inbox
import Network.Ccm.Bsm.Outbox
import Network.Ccm.Types

import Control.Concurrent (forkIO,threadDelay)
import Control.Concurrent.STM
import Control.Exception (catch, IOException, SomeException)
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.Map (Map)
import Data.Word (Word64)
import qualified Data.Map as Map
import qualified Data.Store as Store
import Network.Simple.TCP

type MsgSize = Int

sizeMsgSize :: Int
sizeMsgSize = case (Store.size :: Store.Size MsgSize) of
  Store.VarSize _ -> error "MsgSize has variable Store size"
  Store.ConstSize i -> i

data MyAddr
  = MyAddr { myAddrHost :: HostName
           , myAddrPort :: ServiceName
           }
  deriving (Show)

runServer :: Debugger -> MyAddr -> BsmInbox -> Map NodeId (BsmOutbox,TVar Bool) -> IO ()
runServer dbg myaddr inbox outboxes = do
  debug dbg "Opening server..."
  serve (Host (myAddrHost myaddr)) (myAddrPort myaddr) $ \(sock,addr) -> do
    r <- acceptConnection dbg inbox sock addr
    case r of
      Just target -> case Map.lookup target outboxes of
        Just (ob,status) -> do
          forkIO $ runSender dbg target ob sock
          atomically $ writeTVar status True
          catch (runReceiver dbg target inbox sock) $ \e -> do
            error $ "Caught: " ++ show (e :: IOException)
        Nothing -> debug dbg $ "No outbox for " ++ show target
      Nothing -> return ()

-- testServer = do
--   c <- newBsmInbox
--   out <- newBsmOutbox
--   runServer mkPrinterDbg (MyAddr "127.0.0.1" "8099") c out
--   putStrLn "Test..."

acceptConnection :: Debugger -> BsmInbox -> Socket -> SockAddr -> IO (Maybe NodeId)
acceptConnection dbg inbox sock addr = do
  bs <- recvUntil sock nodeIdSize
  case bs of
    Just bs -> do
      let nid = Store.decodeEx bs
      debug dbg $ "Accepted connection from " ++ show nid
      return (Just nid)
    Nothing -> do
      debug dbg $ "Failed to init connection from " ++ show addr
      return Nothing

runReceiver :: Debugger -> NodeId -> BsmInbox -> Socket -> IO ()
runReceiver dbg i inbox sock = do
  bs <- recvUntil sock sizeMsgSize
  case bs of
    Just bs -> do
      let n = Store.decodeEx bs
      bs2 <- recvUntil sock $ fromIntegral (n :: MsgSize)
      case bs2 of
        Just bs2 -> do
          atomically $ writeBsmInbox inbox (i,bs2)
          let s = Store.decodeEx bs2
          debug dbg $ "[" ++ show i ++ "]: " ++ s
          runReceiver dbg i inbox sock
        Nothing ->
          debug dbg $ "Connection closed (mid-msg): " ++ show i
    Nothing ->
      debug dbg $ "Connection closed: " ++ show i

runSender :: Debugger -> NodeId -> BsmOutbox -> Socket -> IO ()
runSender d i outbox sock = do
  bs <- atomically $ readBsmOutbox outbox
  let n = ByteString.length bs
  let sendAction = do
        send sock (Store.encode (n :: MsgSize) <> bs)
        debug d $ "Sent " ++ show n ++ "b message to " ++ show i  
  catch sendAction $ \e ->
    debug d $ "[Send] Caught: " ++ show (e :: SomeException)
  atomically $ doneBsmOutbox outbox
  runSender d i outbox sock

recvUntil :: Socket -> Int -> IO (Maybe ByteString)
recvUntil sock n = do
  bs <- recv sock n
  case bs of
    Just bs | ByteString.length bs > n -> error "recvUntil: Too many bytes?"
    Just bs | ByteString.length bs == n -> return $ Just bs
    Just bs | ByteString.length bs < n ->
      fmap (fmap (bs <>)) (recvUntil sock (n - ByteString.length bs))
    Nothing -> return Nothing

runClient :: Int -> Debugger -> NodeId -> NodeId -> MyAddr -> BsmInbox -> BsmOutbox -> TVar Bool -> IO ()
runClient delay dbg selfNode targetNode targetAddr inbox outbox status =
  catch (runClient' dbg selfNode targetNode targetAddr inbox outbox status) $ \e -> do
    -- putStrLn $ "Caught: " ++ show (e :: IOException)
    let _ = e :: IOException
    threadDelay delay
    -- putStrLn $ "Retrying connection to " ++ show targetNode
    runClient (delay * 2) dbg selfNode targetNode targetAddr inbox outbox status

runClient' :: Debugger -> NodeId -> NodeId -> MyAddr -> BsmInbox -> BsmOutbox -> TVar Bool -> IO ()
runClient' dbg selfNode targetNode (MyAddr host port) inbox outbox status =
  connect host port $ \(sock,_) -> do
    -- putStrLn $ "Connected to " ++ show targetNode
    send sock (Store.encode selfNode)
    forkIO $ runSender dbg targetNode outbox sock
    atomically $ writeTVar status True
    runReceiver dbg targetNode inbox sock
