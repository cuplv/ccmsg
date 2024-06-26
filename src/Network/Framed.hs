{-# LANGUAGE FlexibleContexts #-}

-- | This module implements a length-framed message transport service
-- over TCP sockets.  Each message is a sequence of bytes preceded by
-- its length, encoded as a big-endian 16-bit unsigned integer.
module Network.Framed
  (
  -- * Lazy Operations
    send
  , recv
  , recvUntil
  -- * Strict Operations
  , sendStrict
  , recvStrict
  , recvUntilStrict
  -- * Exceptions
  , Exception (..)
  -- * Re-exports
  , Word16
  ) where

import qualified Control.Exception as E
import Control.Monad.Except
import Control.Monad.IO.Class
import qualified Data.ByteString as SBS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Builder as Builder
import Data.Int (Int64)
import Data.Word (Word16)
import qualified Data.List as List
import qualified Data.Serialize.Get as Cereal
import qualified Network.Simple.TCP as TCP

word16Size :: Word16
word16Size = 2

-- | Receive @n@ bytes from the socket, or return @'Left' m@ if the
-- socket is closed or end-of-input is reached early after only @m@
-- bytes.
recvUntil :: (MonadError Exception m, MonadIO m) => TCP.Socket -> Word16 -> m (Either Word16 LBS.ByteString)
recvUntil sock n = fmap LBS.fromChunks <$> recvUntilChunks sock n

-- | Strict 'Data.ByteString.ByteString' version of 'recvUntil'.
recvUntilStrict :: (MonadError Exception m, MonadIO m) => TCP.Socket -> Word16 -> m (Either Word16 SBS.ByteString)
recvUntilStrict sock n = fmap LBS.toStrict <$> recvUntil sock n

recvUntilChunks :: (MonadError Exception m, MonadIO m) => TCP.Socket -> Word16 -> m (Either Word16 [SBS.ByteString])
recvUntilChunks sock n = do
  let ni = fromIntegral n
  bs <- TCP.recv sock ni
  case bs of
    -- If too many bytes are received, it's an error in TCP.recv
    Just bs | SBS.length bs > ni -> error "recvUntil: Too many bytes?"
    Just bs | SBS.length bs == ni -> return $ Right [bs]
    Just bs | SBS.length bs < ni -> do
      result <- recvUntilChunks sock (n - fromIntegral (SBS.length bs))
      case result of
        Right chunks -> return $ Right (bs : chunks)
        Left n' -> return $ Left (fromIntegral (SBS.length bs) + n')
    Nothing -> return $ Left 0

checkedInt64ToWord16 :: Int64 -> Maybe Word16
checkedInt64ToWord16 i
  | i <= fromIntegral (maxBound :: Word16) = Just $ fromIntegral i
  | otherwise = Nothing

-- | Send a message to the socket.  This sends a big-endian encoded
-- 'Data.Word.Word16' declaring the length of the content, followed by
-- the provided content.  Note that the content cannot have more bytes
-- than 'maxBound' of 'Data.Word.Word16'.
send
  :: (MonadError Exception m, MonadIO m)
  => TCP.Socket
  -> LBS.ByteString
  -> m ()
send sock content = do
  n <- case checkedInt64ToWord16 . LBS.length $ content of
    Just n -> return n
    Nothing -> throwError $ Oversize (LBS.length content)
  let
    nb = Builder.word16BE n
    bytes = Builder.toLazyByteString $ nb <> Builder.lazyByteString content
    action = Right <$> TCP.sendLazy sock bytes
  r <- liftIO $ E.catch action $ \e -> return (Left e)
  case r of
    Right bs -> return bs
    Left e -> throwError e

-- | Strict 'Data.ByteString.ByteString' version of 'send'.
sendStrict
  :: (MonadError Exception m, MonadIO m)
  => TCP.Socket
  -> SBS.ByteString
  -> m ()
sendStrict sock content = send sock (LBS.fromStrict content)

-- | Receive a message from the socket, or a 'RecvException' if
-- unsuccessful.
recv
  :: (MonadError Exception m, MonadIO m)
  => TCP.Socket
  -> m LBS.ByteString
recv sock = do
  sizeB <- recvUntilStrict sock word16Size
  case sizeB of
    Right bs -> do
      case Cereal.runGet Cereal.getWord16be bs of
        Right n -> do
          contentM <- recvUntil sock n
          case contentM of
            Right bs -> return bs
            Left n' -> throwError $ ContentEOF n n'
        Left e -> do
          throwError $ LengthDecode e
    Left n -> throwError $ LengthEOF n word16Size

-- | Strict 'Data.ByteString.ByteString' version of 'recv'.
recvStrict
  :: (MonadError Exception m, MonadIO m)
  => TCP.Socket
  -> m SBS.ByteString
recvStrict sock = LBS.toStrict <$> recv sock

data Exception
  = BrokenPipe
  | ConnectionResetByPeer
  | ConnectionRefused
  | Oversize Int64
    -- ^ Attempted to send a message with content size beyond
    -- 'maxBound' of 'Word16'.
  | ContentEOF Word16 Word16
    -- ^ The socket closed or EOF'd before the expected size message
    -- content was received.  The first 'Word16' is how many bytes were
    -- received, and the second is how many were expected.
  | LengthEOF Word16 Word16
    -- ^ The socket closed or EOF'd before the message's length was
    -- received.  The first 'Word16' is how many bytes were received, and
    -- the second is how many were expected.
  | LengthDecode String
    -- ^ The message's leading length, a 'Word16' in big-endian order,
    -- could not be decoded.  The 'String' provides details.
  | ContentException String
    -- ^ There was a problem with the message's content.  The 'String'
    -- provides details.
  deriving (Show,Eq,Ord)

instance E.Exception Exception where
  fromException e
    | List.isInfixOf "(Broken pipe)" (show e) = Just BrokenPipe
    | List.isInfixOf "(Connection refused)" (show e) = Just ConnectionRefused
    | List.isInfixOf "(Connection reset by peer)" (show e) = Just ConnectionResetByPeer
    | otherwise = Nothing
