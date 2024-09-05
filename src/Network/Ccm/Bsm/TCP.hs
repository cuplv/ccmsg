module Network.Ccm.Bsm.TCP where

import Control.Monad.DebugLog
import Network.Ccm.Bsm.Internal
import Network.Ccm.Lens
import Network.Ccm.Types

import Control.Concurrent (forkIO,threadDelay)
import Control.Concurrent.STM
import Control.Exception (catch, IOException, SomeException)
import Control.Monad (forever)
import Control.Monad.IO.Class
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

runServer :: MyAddr -> Bsm -> LogIO IO ()
runServer myaddr bsm = do
  dlog ["trace"] "Opening server..."
  serveM <- passLogIOF $ \(sock,addr) -> do
    r <- acceptConnection sock addr
    case r of
      Just target -> case Map.lookup target (bsm^.bsmPeers) of
        Just p -> do
          forkLogIO $ runSender target (p^.peerOutbox) sock
          liftIO.atomically $ writeTVar (p^.peerStatus) PSConnected
          tryM <- passLogIO $
            runReceiver target (bsm^.bsmInbox) sock
          catchM <- passLogIOF $ \e ->
            error $ "Caught: " ++ show (e :: IOException)
          liftIO $ catch tryM catchM
        Nothing -> -- debug (bsm^.bsmDbg) $ "No outbox for " ++ show target
          dlog ["error"] $ "No outbox for " ++ show target
      Nothing -> return ()
  serve (Host (myAddrHost myaddr)) (myAddrPort myaddr) serveM

acceptConnection
  :: Socket
  -> SockAddr
  -> LogIO IO (Maybe NodeId)
acceptConnection sock addr = do
  bs <- liftIO $ recvUntil sock nodeIdSize
  case bs of
    Just bs -> do
      let nid = Store.decodeEx bs
      dlog ["trace"] $ "Accepted connection from " ++ show nid
      return (Just nid)
    Nothing -> do
      dlog ["error"] $ "Failed to init connection from " ++ show addr
      return Nothing

runReceiver
  :: NodeId
  -> TBQueue (NodeId, ByteString)
  -> Socket
  -> LogIO IO ()
runReceiver i inbox sock = do
  bs <- liftIO $ recvUntil sock sizeMsgSize
  case bs of
    Just bs -> do
      let n = Store.decodeEx bs
      bs2 <- liftIO $ recvUntil sock $ fromIntegral (n :: MsgSize)
      case bs2 of
        Just bs2 -> do
          liftIO $ atomically $ writeTBQueue inbox (i,bs2)
          -- let s = Store.decodeEx bs2
          dlog ["debug"] $ "Received msg from " ++ show i
          runReceiver i inbox sock
        Nothing ->
          dlog ["error"] $ "Connection closed (mid-msg): " ++ show i
    Nothing ->
      dlog ["error"] $ "Connection closed: " ++ show i

runSender :: NodeId -> TBQueue PeerMessage -> Socket -> LogIO IO ()
runSender i outbox sock = do
  PMData bs <- liftIO.atomically $ readTBQueue outbox
  let n = ByteString.length bs
  let sendAction = do
        send sock (Store.encode (n :: MsgSize) <> bs)
        -- debug d $ "Sent " ++ show n ++ "b message to " ++ show i
        dlog ["debug"] $ "Sent " ++ show n ++ "b message to " ++ show i
  tryM <- passLogIO sendAction
  catchM <- passLogIOF $ \e ->
    dlog ["error"] $ "runSender: Caught: " ++ show (e :: SomeException)
  liftIO $ catch tryM catchM
  -- catch sendAction $ \e ->
  --   -- debug d $ "[Send] Caught: " ++ show (e :: SomeException)
  --   dlog ["error"] $ "runSender: Caught: " ++ show (e :: SomeException)
  runSender i outbox sock

recvUntil :: Socket -> Int -> IO (Maybe ByteString)
recvUntil sock n = do
  bs <- recv sock n
  case bs of
    Just bs | ByteString.length bs > n -> error "recvUntil: Too many bytes?"
    Just bs | ByteString.length bs == n -> return $ Just bs
    Just bs | ByteString.length bs < n ->
      fmap (fmap (bs <>)) (recvUntil sock (n - ByteString.length bs))
    Nothing -> return Nothing

runClient
  :: Int
  -> Bsm
  -> NodeId
  -> MyAddr
  -> LogIO IO ()
runClient delay bsm targetNode targetAddr = do
  let peer = case Map.lookup targetNode (bsm^.bsmPeers) of
        Just p -> p
        Nothing -> error $ "runClient on non-existent peer: " ++ show targetNode
  tryM <- passLogIO $ runClient' bsm targetNode targetAddr peer
  catchM <- passLogIOF $ \e -> do
    dlog ["error"] $
      "Connection error to "
      ++ show targetNode
      ++ ": "
      ++ show (e :: IOException)
    liftIO $ threadDelay delay
  forever.liftIO $ catch tryM catchM
  -- forever $
  --   catch (runClient' bsm targetNode targetAddr peer) $ \e -> do
  --     -- putStrLn $ "Caught: " ++ show (e :: IOException)
  --     let _ = e :: IOException
  --     threadDelay delay
  --     -- putStrLn $ "Retrying connection to " ++ show targetNode
  --     -- runClient (delay * 2) dbg selfNode targetNode targetAddr peer inbox

runClient'
  :: Bsm
  -> NodeId
  -> MyAddr
  -> Peer
  -> LogIO IO ()
runClient' bsm targetNode (MyAddr host port) peer = do
  m <- passLogIOF $ \(sock,_) -> do
    -- putStrLn $ "Connected to " ++ show targetNode
    send sock (Store.encode (bsm^.bsmSelf))
    forkLogIO $ runSender targetNode (peer^.peerOutbox) sock
    liftIO.atomically $ writeTVar (peer^.peerStatus) PSConnected
    runReceiver targetNode (bsm^.bsmInbox) sock
  liftIO $ connect host port m
