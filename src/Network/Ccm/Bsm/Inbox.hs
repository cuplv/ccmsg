module Network.Ccm.Bsm.Inbox
  ( BsmInbox
  , newBsmInbox
  , readBsmInbox
  , readManyBsmInbox
  , tryReadManyBsmInbox
  , writeBsmInbox
  , isEmptyBsmInbox
  ) where

import Network.Ccm.Types
import Network.Ccm.Bsm.Internal

import Control.Concurrent.STM
import Data.ByteString (ByteString)

data BsmInbox = BsmInbox (TBQueue (NodeId, ByteString))

{-| Read all available messages, or block if none. -}
readManyBsmInbox :: BsmInbox -> STM [(NodeId, ByteString)]
readManyBsmInbox (BsmInbox chan) = do
  e <- isEmptyTBQueue chan
  if e
    then retry
    else flushTBQueue chan

{-| Read all available messages, or return @[]@ if there are none. -}
tryReadManyBsmInbox :: BsmInbox -> STM [(NodeId, ByteString)]
tryReadManyBsmInbox (BsmInbox chan) = flushTBQueue chan

isEmptyBsmInbox :: BsmInbox -> STM Bool
isEmptyBsmInbox (BsmInbox chan) = isEmptyTBQueue chan

readBsmInbox :: BsmInbox -> STM (NodeId, ByteString)
readBsmInbox (BsmInbox chan) = readTBQueue chan

writeBsmInbox :: BsmInbox -> (NodeId, ByteString) -> STM ()
writeBsmInbox (BsmInbox chan) m = writeTBQueue chan m

newBsmInbox :: IO BsmInbox
newBsmInbox = BsmInbox <$> newTBQueueIO bsmBoxBound
