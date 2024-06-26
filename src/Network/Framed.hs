{-# LANGUAGE FlexibleContexts #-}

module Network.Framed
  ( send
  , sendStrict
  , recv
  , RecvException (..)
  , NetworkException (..)
  ) where

import qualified Control.Exception as E
import Control.Monad.Except
import Control.Monad.IO.Class
import qualified Data.ByteString as SBS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Builder as Builder
import Data.Int (Int32)
import qualified Data.List as List
import qualified Data.Serialize.Get as Cereal
import qualified Network.Simple.TCP as TCP

int32Size :: Int
int32Size = 4

-- | Receive N bytes from the socket, or return 'Nothing' if the
-- socket is closed or end-of-input is reached before N bytes.
recvUntil :: (MonadIO m) => TCP.Socket -> Int -> m (Either Int SBS.ByteString)
recvUntil sock n = do
  bs <- TCP.recv sock n
  case bs of
    -- If too many bytes are received, it's an error in TCP.recv
    Just bs | SBS.length bs > n -> error "recvUntil: Too many bytes?"
    Just bs | SBS.length bs == n -> return $ Right bs
    -- Just bs | SBS.length bs < n ->
    --   fmap (fmap (bs <>)) (recvUntil sock (n - SBS.length bs))
    Just bs | SBS.length bs < n -> do
      result <- recvUntil sock (n - SBS.length bs)
      case result of
        Right bs' -> return $ Right (bs <> bs')
        Left n' -> return $ Left (SBS.length bs + n')
    Nothing -> return $ Left 0

-- | Send a message to the socket.  This sends a big-endian encoded
-- 'Int32' declaring the length of the content, followed by the
-- provided content.
send
  :: (MonadError NetworkException m, MonadIO m)
  => TCP.Socket
  -> LBS.ByteString
  -> m ()
send sock content = do
  let
    n = Builder.int32BE . fromIntegral . LBS.length $ content
    bytes = Builder.toLazyByteString $ n <> Builder.lazyByteString content
    action = Right <$> TCP.sendLazy sock bytes
  r <- liftIO $ E.catch action $ \e -> return (Left e)
  case r of
    Right bs -> return bs
    Left e -> throwError e
    
sendStrict
  :: (MonadError NetworkException m, MonadIO m)
  => TCP.Socket
  -> SBS.ByteString
  -> m ()
sendStrict sock content = send sock (LBS.fromStrict content)

data RecvException
  = SocketClosed Int Int
    -- ^ The socket closed or EOF'd before the expected size message
    -- was received.  The first 'Int' is how many bytes were received,
    -- and the second is how many were expected.
  | LengthDecode String
    -- ^ The message's leading length, an 'Int32' in big-endian order,
    -- could not be decoded.  The 'String' provides details.
  | ContentDecode String
    -- ^ The message's content could not be decoded.
  | RecvNetwork NetworkException
    -- ^ A network exception arose
  deriving (Show,Eq,Ord)

-- | Receive a message from the socket, or a 'RecvException' if unsuccessful.
recv
  :: (MonadError RecvException m, MonadIO m)
  => TCP.Socket
  -> m SBS.ByteString
recv sock = do
  sizeB <- recvUntil sock int32Size
  case sizeB of
    Right bs -> do
      case Cereal.runGet Cereal.getInt32be bs of
        Right n -> do
          contentM <- recvUntil sock (fromIntegral n)
          case contentM of
            Right bs -> return bs
            Left n' -> throwError $ SocketClosed (fromIntegral n) n'
        Left e -> do
          throwError $ LengthDecode e
    Left n -> throwError $ SocketClosed int32Size n

data NetworkException
  = BrokenPipe
  | ConnectionResetByPeer
  | ConnectionRefused
  deriving (Show,Eq,Ord)

instance E.Exception NetworkException where
  fromException e
    | List.isInfixOf "(Broken pipe)" (show e) = Just BrokenPipe
    | List.isInfixOf "(Connection refused)" (show e) = Just ConnectionRefused
    | List.isInfixOf "(Connection reset by peer)" (show e) = Just ConnectionResetByPeer
    | otherwise = Nothing
  displayException e = case e of
    BrokenPipe -> "Broken pipe"
    ConnectionResetByPeer -> "Connection reset by peer"
    ConnectionRefused -> "Connection refused"
