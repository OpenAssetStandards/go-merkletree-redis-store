package merkleredis

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/go-redis/redis/v9"
	"github.com/iden3/go-merkletree-sql/v2"
)

const merkleTreeNodeBase = "mt_n_"
const merkleTreeRootBase = "mt_r_"

// TODO: upsert or insert?
const upsertStmt = `INSERT INTO mt_nodes (mt_id, key, type, child_l, child_r, entry) VALUES ($1, $2, $3, $4, $5, $6) ` +
	`ON CONFLICT (mt_id, key) DO UPDATE SET type = $3, child_l = $4, child_r = $5, entry = $6`

const updateRootStmt = `INSERT INTO mt_roots (mt_id, key) VALUES ($1, $2) ` +
	`ON CONFLICT (mt_id) DO UPDATE SET key = $2`

func readUint32LE(d []byte, index int) uint32 {
	return uint32(d[index+3])<<24 | uint32(d[index+2])<<16 | uint32(d[index+1])<<8 |
		uint32(d[index])
}
func writeUint32LE(d []byte, index int, value uint32) {
	d[index] = byte(value & 0xff)
	d[index+1] = byte((value >> 8) & 0xff)
	d[index+2] = byte((value >> 16) & 0xff)
	d[index+3] = byte((value >> 24) & 0xff)
}
func NewMerkleRedisStorage(client *redis.Client, prefix string) *Storage {
	return &Storage{
		db:           client,
		nodeIdPrefix: merkleTreeNodeBase + prefix + "_",
		rootId:       merkleTreeRootBase + prefix,
	}
}

// Storage implements the db.Storage interface
type Storage struct {
	db           *redis.Client
	nodeIdPrefix string
	rootId       string
	currentRoot  *merkletree.Hash
}

type NodeItem struct {
	Type byte   `db:"type"`
	Key  []byte `db:"key"`
	// Type is the type of node in the tree.
	// ChildL is the left child of a middle node.
	ChildL []byte `db:"child_l"`
	// ChildR is the right child of a middle node.
	ChildR []byte `db:"child_r"`
	// Entry is the data stored in a leaf node.
	Entry []byte `db:"entry"`
}

type RootItem struct {
	MTId uint64 `db:"mt_id"`
	Key  []byte `db:"key"`
}

func bytesToNodeItem(d []byte) (*NodeItem, error) {
	dLen := len(d)
	if dLen < 17 {
		return nil, fmt.Errorf("corrupted merkle node: invalid header")
	}
	keyLen := int(readUint32LE(d, 1))
	childLLen := int(readUint32LE(d, 5))
	childRLen := int(readUint32LE(d, 9))
	entryLen := int(readUint32LE(d, 13))
	if (keyLen + childLLen + childRLen + entryLen + 17) > len(d) {
		return nil, fmt.Errorf("corrupted merkle node: overflow")
	}

	ni := &NodeItem{
		Type: d[0],
	}
	p := 17
	ni.Key = d[p:(p + keyLen)]
	p += keyLen
	ni.ChildL = d[p:(p + childLLen)]
	p += childLLen
	ni.ChildR = d[p:(p + childRLen)]
	p += childRLen
	ni.Entry = d[p:(p + entryLen)]

	return ni, nil

}
func nodeItemToBytes(n *NodeItem) []byte {
	d := make([]byte, 17+len(n.Key)+len(n.ChildL)+len(n.ChildR)+len(n.Entry))
	d[0] = n.Type
	pos := 17
	if n.Key != nil {
		writeUint32LE(d, 1, uint32(len(n.Key)))
		copy(d[pos:], n.Key)
		pos += len(n.Key)
	} else {
		writeUint32LE(d, 1, 0)
	}
	if n.ChildL != nil {
		writeUint32LE(d, 5, uint32(len(n.ChildL)))
		copy(d[pos:], n.ChildL)
		pos += len(n.ChildL)
	} else {
		writeUint32LE(d, 5, 0)
	}
	if n.ChildR != nil {
		writeUint32LE(d, 9, uint32(len(n.ChildR)))
		copy(d[pos:], n.ChildR)
		pos += len(n.ChildR)
	} else {
		writeUint32LE(d, 9, 0)
	}
	if n.Entry != nil {
		writeUint32LE(d, 13, uint32(len(n.Entry)))
		copy(d[pos:], n.ChildR)
	} else {
		writeUint32LE(d, 13, 0)
	}
	return d
}
func (s *Storage) getRedisNodeIdForMerkleKey(key []byte) string {
	return s.nodeIdPrefix + hex.EncodeToString(key)
}

// Get retrieves a value from a key in the db.Storage
func (s *Storage) Get(ctx context.Context,
	key []byte) (*merkletree.Node, error) {

	res := s.db.Get(ctx, s.getRedisNodeIdForMerkleKey(key))
	if res.Err() == redis.Nil {
		return nil, merkletree.ErrNotFound
	} else if res.Err() != nil {
		return nil, res.Err()
	} else {
		d, err := hex.DecodeString(res.Val())
		if err != nil {
			return nil, fmt.Errorf("corrupt key hex")
		}
		item, err := bytesToNodeItem(d)
		if err != nil {
			return nil, err
		}
		node, err := item.Node()
		if err != nil {
			return nil, err
		}
		return node, nil
	}

}

func (s *Storage) Put(ctx context.Context, key []byte,
	node *merkletree.Node) error {

	item := &NodeItem{Key: key}

	if node.ChildL != nil {
		item.ChildL = append(item.ChildL, node.ChildL[:]...)
	}

	if node.ChildR != nil {
		item.ChildR = append(item.ChildR, node.ChildR[:]...)
	}

	if node.Entry[0] != nil && node.Entry[1] != nil {
		item.Entry = append(node.Entry[0][:], node.Entry[1][:]...)
	}

	res := s.db.Set(ctx, s.getRedisNodeIdForMerkleKey(key), hex.EncodeToString(nodeItemToBytes(item)), 0)
	return res.Err()
}

// GetRoot retrieves a merkle tree root hash in the interface db.Tx
func (s *Storage) GetRoot(ctx context.Context) (*merkletree.Hash, error) {
	var root merkletree.Hash
	if s.currentRoot != nil {
		copy(root[:], s.currentRoot[:])
		return &root, nil
	}

	res := s.db.Get(ctx, s.rootId)
	if res.Err() == redis.Nil {
		return nil, merkletree.ErrNotFound
	} else if res.Err() != nil {
		return nil, res.Err()
	} else {
		d, err := hex.DecodeString(res.Val())
		if err != nil {
			return nil, fmt.Errorf("corrupt root hex")
		}
		if s.currentRoot == nil {
			s.currentRoot = &merkletree.Hash{}
		}
		copy(s.currentRoot[:], d[:])
		copy(root[:], s.currentRoot[:])
		return &root, nil
	}
}

func (s *Storage) SetRoot(ctx context.Context, hash *merkletree.Hash) error {
	if s.currentRoot == nil {
		s.currentRoot = &merkletree.Hash{}
	}
	copy(s.currentRoot[:], hash[:])
	res := s.db.Set(ctx, s.rootId, hex.EncodeToString(hash[:]), 0)
	if res.Err() != nil {
		return newErr(res.Err(), "failed to update current root hash")
	}
	return nil
}

func (item *NodeItem) Node() (*merkletree.Node, error) {
	node := merkletree.Node{
		Type: merkletree.NodeType(item.Type),
	}
	if item.ChildL != nil {
		node.ChildL = &merkletree.Hash{}
		copy(node.ChildL[:], item.ChildL[:])
	}
	if item.ChildR != nil {
		node.ChildR = &merkletree.Hash{}
		copy(node.ChildR[:], item.ChildR[:])
	}
	if len(item.Entry) > 0 {
		if len(item.Entry) != 2*merkletree.ElemBytesLen {
			return nil, merkletree.ErrNodeBytesBadSize
		}
		node.Entry = [2]*merkletree.Hash{{}, {}}
		copy(node.Entry[0][:], item.Entry[0:32])
		copy(node.Entry[1][:], item.Entry[32:64])
	}
	return &node, nil
}

// KV contains a key (K) and a value (V)
type KV struct {
	MTId uint64
	K    []byte
	V    merkletree.Node
}

type storageError struct {
	err error
	msg string
}

func (err storageError) Error() string {
	return err.msg + ": " + err.err.Error()
}

func (err storageError) Unwrap() error {
	return err.err
}

func newErr(err error, msg string) error {
	return storageError{err, msg}
}
