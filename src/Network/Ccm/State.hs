{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.State
  ( CcmState
  , ccmStats
  , CcmST
  , newCcmState
  , recordSend
  , outputClock
  , MsgId
  , AppMsg
  , SimpleAppMsg (..)
  , mkSimpleAppMsg
  , mkCausalAppMsg
  , msgClock
  , msgPayload
  , CausalError (..)
  , showCausalError'
  , Stats
  , totalOutOfOrder
  , totalInOrder
  , CacheMode (..)
  , punchOutputClock
  , revive
  , cache
  , mcWaiting
  , msgId
  , cacheDelivered
  , deferMsg
  , OutputResult (..)
  , InputResult (..)
  , punchInputClock
  ) where

import Network.Ccm.Bsm
import Network.Ccm.Lens
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Concurrent (ThreadId)
import Control.Concurrent.STM
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Foldable (foldlM)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromJust)
import Data.Sequence (Seq,(<|),(|>))
import qualified Data.Sequence as Seq
import Data.Store
import Data.Store.TH
import Data.Traversable (for)
import GHC.Exts (toList,fromList)

type MsgId = (NodeId, SeqNum)

data CacheMode
  = CacheNone -- ^ Do not cache messages, disabling retransmission
  | CacheTemp -- ^ Cache all messages until they are universally delivered
  | CacheForever -- ^ Cache all messages forever
  deriving (Show,Eq,Ord)

data AppMsg
  = AppMsg { _msgClock :: VClock
           , _msgPayload :: ByteString
           }
  deriving (Show,Eq,Ord)

makeStore ''AppMsg

makeLenses ''AppMsg

data SimpleAppMsg
  = SimpleAppMsg ByteString
  deriving (Show,Eq,Ord)

makeStore ''SimpleAppMsg

mkSimpleAppMsg :: ByteString -> SimpleAppMsg
mkSimpleAppMsg = SimpleAppMsg

mkCausalAppMsg :: VClock -> ByteString -> AppMsg
mkCausalAppMsg = AppMsg

data CausalError
  = CausalError
    { errorMsgSender :: NodeId
    , errorLocalClock :: VClock
    , errorMsgClock :: VClock
    }
  deriving (Eq,Ord)

instance Show CausalError where
  show e = 
   "[!]Causal: "
    ++ show (errorMsgSender e)
    ++ " "
    ++ show (errorMsgClock e)
    ++ " => "
    ++ show (errorLocalClock e)

{-| Show causal error, including receiver's ID -}
showCausalError' :: NodeId -> CausalError -> String
showCausalError' n e =
  "[!]Causal: "
  ++ show (errorMsgSender e)
  ++ " "
  ++ show (errorMsgClock e)
  ++ " => "
  ++ show n
  ++ " "
  ++ show (errorLocalClock e)

{-| Sequence number of a message, in terms of its sender and its clock. -}
msgSeqNum :: NodeId -> AppMsg -> SeqNum
msgSeqNum sender msg = nextNum sender (msg ^. msgClock)

msgId :: NodeId -> AppMsg -> MsgId
msgId sender msg = (sender, nextNum sender (msg^.msgClock))

data MsgCache
  = MsgCache
    { _mcDeleted :: PostCount
    , _mcDelivered :: Seq AppMsg
    , _mcWaiting :: Seq AppMsg
    }
    deriving (Show, Eq, Ord)

makeLenses ''MsgCache

newMsgCache :: MsgCache
newMsgCache = MsgCache 0 Seq.Empty Seq.Empty

data Stats
  = Stats
    { _totalOutOfOrder :: Int
    , _totalInOrder :: Int
    }

makeLenses ''Stats

data Block
  = Block
    { _blSeqNum :: SeqNum
    , _blNodeIds :: Seq NodeId
    , _blTimer :: Maybe (TMVar (), ThreadId)
    }

makeLenses ''Block

newBlock :: SeqNum -> NodeId -> Block
newBlock sn i = Block
  { _blSeqNum = sn
  , _blNodeIds = Seq.singleton i
  , _blTimer = Nothing
  }

data CcmState
  = CcmState
    { _ccmCache :: Map NodeId MsgCache
    , _ccmBlocks :: Map NodeId Block
    , _ccmReady :: Seq NodeId
    , _ccmPeerClocks :: Map NodeId VClock
    , _outputClock :: VClock
    , _inputClock :: VClock
    , _ccmStats :: Stats
    , _ccmCacheMode :: CacheMode
    }

type CcmST m = StateT CcmState m

makeLenses ''CcmState

blocks :: NodeId -> Lens' CcmState (Maybe Block)
blocks i = ccmBlocks . at i

cache :: NodeId -> Lens' CcmState MsgCache
cache i = ccmCache . at i . non newMsgCache

newCcmState :: CacheMode -> CcmState
newCcmState cacheMode = CcmState
  { _ccmCache = Map.empty
  , _ccmBlocks = Map.empty
  , _ccmReady = Seq.Empty
  , _ccmPeerClocks = Map.empty
  , _outputClock = zeroClock
  , _inputClock = zeroClock
  , _ccmStats = Stats
    { _totalOutOfOrder = 0
    , _totalInOrder = 0
    }
  , _ccmCacheMode = cacheMode
  }

{- | Cache a delivered message for later retransmission requests, if
   configured to do so. -}
cacheDelivered
  :: (Monad m)
  => (NodeId, AppMsg)
  -> CcmST m ()
cacheDelivered (sender, msg) = do
  m <- use ccmCacheMode
  case m of
    CacheNone -> return ()
    _ -> cache sender . mcDelivered %= (|> msg)

{-| Run @deferMsgId i m@ when you receive a message from @n@ that
  cannot be delivered until after @m@ has been delivered. -}
deferMsg :: (Monad m) => NodeId -> MsgId -> CcmST m ()
deferMsg i1 (i2,sn2) =
  blocks i2 %= \case
    Just b -> Just $ b
      & blSeqNum %~ (min sn2)
      & blNodeIds %~ (|> i1)
    -- Just (sn,d) -> Just (min sn sn2, d |> i1)
    Nothing -> Just $ newBlock sn2 i1

data OutputResult
  = OCOutput -- ^ Post has been marked as output.
  | OCDefer MsgId -- ^ Post cannot be output, because it has some non-output dependencies, and one of them has the given 'MsgId'.

{- | @punchOutputClock i v@ attempts to update the local clock to
   include a new message from process @i@, which has dependencies @v@.

   The accept-clock is only modified when 'ClockAccepted' is returned,
   but the witness-clock may be modified when either 'ClockAccepted'
   or 'ClockRejected' are returned. -}
punchOutputClock :: (Monad m) => NodeId -> VClock -> CcmST m OutputResult
punchOutputClock msgSender msgClock = do
  accept <- use outputClock
  case leVC' msgClock accept of
    Right () -> do
      outputClock %= tick msgSender
      return OCOutput
    Left m ->
      return $ OCDefer m

{-| Record the sending of a new message, trusting that the new message's
  clock is satisfied by the local clock.

  TODO: The message should be cached for later re-send requests.  For
  now, only the local clock is modified. -}
recordSend :: (Monad m) => (NodeId, AppMsg) -> CcmST m ()
recordSend (sender,msg) = do
  outputClock %= tick sender
  cacheDelivered (sender,msg)

{-| Return any 'MsgId's that were waiting on the given 'MsgId', removing
  them from the internal waiting map. -}
revive :: (Monad m) => MsgId -> CcmST m (Seq NodeId)
revive (sender,sn) = do
  bs <- use (blocks sender)
  case bs of
    Just b | (b^.blSeqNum) <= sn -> do
      blocks sender .= Nothing
      return (b^.blNodeIds)
    _ -> return Seq.Empty

data InputResult
  = ICInput
  | ICRepeat
  | ICSkipped SeqNum

{-| Try to record a newly-receive post on the input-clock.

  If successful, the input-clock is modified to include the new post,
  and 'ICInput' is returned.  Otherwise, the clock is not modified.
-}
punchInputClock :: (Monad m) => NodeId -> VClock -> CcmST m InputResult
punchInputClock sender postClock = do
  ic <- use inputClock
  let
    n = nextNum sender postClock
    m = nextNum sender ic
  if
    | n == m -> do
        inputClock %= tick sender
        return ICInput
    | n < m -> return ICRepeat
    | n > m -> return $ ICSkipped m
