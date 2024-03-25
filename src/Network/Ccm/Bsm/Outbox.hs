module Network.Ccm.Bsm.Outbox
  ( BsmOutbox
  , newBsmOutbox
  , readBsmOutbox
  , doneBsmOutbox
  , writeBsmOutbox
  , emptyOutbox
  ) where

import Network.Ccm.Bsm.Internal

import Control.Concurrent.STM
import Data.ByteString (ByteString)

data BsmOutbox
  = BsmOutbox
    { outboxQueue :: TBQueue ByteString
    , outboxSending :: TVar Bool
    }

readBsmOutbox :: BsmOutbox -> STM ByteString
readBsmOutbox ob = do
  bs <- readTBQueue (outboxQueue ob)
  writeTVar (outboxSending ob) True
  return bs

doneBsmOutbox :: BsmOutbox -> STM ()
doneBsmOutbox ob = writeTVar (outboxSending ob) False

writeBsmOutbox :: BsmOutbox -> ByteString -> STM ()
writeBsmOutbox (BsmOutbox chan _) bs = writeTBQueue chan bs

newBsmOutbox :: IO BsmOutbox
newBsmOutbox = do
  q <- newTBQueueIO bsmBoxBound
  v <- newTVarIO False
  return $ BsmOutbox
    { outboxQueue = q
    , outboxSending = v
    }

emptyOutbox :: BsmOutbox -> STM Bool
emptyOutbox ob = do
  (&&)
    <$> isEmptyTBQueue (outboxQueue ob)
    <*> (not <$> readTVar (outboxSending ob))
