{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.State
  ( CcmState
  , ccmStats
  , CcmST
  , newCcmState
  , recordSend
  , acceptClock
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
  , punchClock
  , revive
  , cache
  , mcWaiting
  , msgId
  , cacheDelivered
  , deferMsg
  , ClockResult (..)
  ) where

import Network.Ccm.Bsm
import Network.Ccm.Lens
import Network.Ccm.Types
import Network.Ccm.VClock

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
    { _mcDeleted :: MsgCount
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

data CcmState
  = CcmState
    { _ccmCache :: Map NodeId MsgCache
    , _ccmBlocks :: Map NodeId (SeqNum, Seq NodeId)
    , _ccmReady :: Seq NodeId
    , _ccmPeerClocks :: Map NodeId VClock
    , _acceptClock :: VClock
    , _witnessClock :: VClock
    , _ccmStats :: Stats
    , _ccmCacheMode :: CacheMode
    }

type CcmST m = StateT CcmState m

makeLenses ''CcmState

blocks :: NodeId -> Lens' CcmState (Maybe (SeqNum, Seq NodeId))
blocks i = ccmBlocks . at i

cache :: NodeId -> Lens' CcmState MsgCache
cache i = ccmCache . at i . non newMsgCache

newCcmState :: CacheMode -> CcmState
newCcmState cacheMode = CcmState
  { _ccmCache = Map.empty
  , _ccmBlocks = Map.empty
  , _ccmReady = Seq.Empty
  , _ccmPeerClocks = Map.empty
  , _acceptClock = zeroClock
  , _witnessClock = zeroClock
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
  blocks i2 %= \v -> case v of
    Just (sn,d) -> Just (min sn sn2, d |> i1)
    Nothing -> Just (sn2, Seq.singleton i1)

data ClockResult
  = ClockAccepted -- ^ Accept clock includes all dependencies.
  | ClockRejected MsgId -- ^ Accept clock is missing dependencies, and one of them has the given 'MsgId'.
  | ClockAlreadyAccepted -- ^ Accept clock already includes the given message.
  | ClockSO -- ^ Witness clock is missing same-sender dependencies.

{- | @punchClock i v@ attempts to update the local clock to include a
   new message from process @i@, which has dependencies @v@.

   The accept-clock is only modified when 'ClockAccepted' is returned,
   but the witness-clock may be modified when either 'ClockAccepted'
   or 'ClockRejected' are returned. -}
punchClock :: (Monad m) => NodeId -> VClock -> CcmST m ClockResult
punchClock msgSender msgClock = do
  witness <- use witnessClock
  let
    msgNum = nextNum msgSender msgClock

  if leNode msgSender msgClock witness
    then do
      witnessClock %= tickTo msgNum msgSender
      accept <- use acceptClock
      case leVC' msgClock accept of
        _ | hasSeen msgSender msgNum accept ->
          return ClockAlreadyAccepted
        Right () -> do
          acceptClock %= tick msgSender
          return ClockAccepted
        Left m ->
          return $ ClockRejected m
    else return ClockSO

{-| Record the sending of a new message, trusting that the new message's
  clock is satisfied by the local clock.

  TODO: The message should be cached for later re-send requests.  For
  now, only the local clock is modified. -}
recordSend :: (Monad m) => (NodeId, AppMsg) -> CcmST m ()
recordSend (sender,msg) = do
  acceptClock %= tick sender
  cacheDelivered (sender,msg)

{-| Return any 'MsgId's that were waiting on the given 'MsgId', removing
  them from the internal waiting map. -}
revive :: (Monad m) => MsgId -> CcmST m (Seq NodeId)
revive (sender,sn) = do
  bs <- use (blocks sender)
  case bs of
    Just (sn',nids) | sn' <= sn -> do
      blocks sender .= Nothing
      return nids
    _ -> return Seq.Empty
