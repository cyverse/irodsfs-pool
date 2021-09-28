package utils

// FileBlockHelper ...
type FileBlockHelper struct {
	BlockSize int
}

func NewFileBlockHelper(blockSize int) *FileBlockHelper {
	return &FileBlockHelper{
		BlockSize: blockSize,
	}
}

// GetBlockIDForOffset returns block index
func (helper *FileBlockHelper) GetBlockIDForOffset(offset int64) int64 {
	blockID := offset / int64(helper.BlockSize)
	return blockID
}

// GetBlockStartOffsetForBlockID returns block start offset
func (helper *FileBlockHelper) GetBlockStartOffsetForBlockID(blockID int64) int64 {
	return int64(blockID) * int64(helper.BlockSize)
}

// GetInBlockOffsetAndLength returns in-block offset and in-block length
func (helper *FileBlockHelper) GetInBlockOffsetAndLength(offset int64, length int) (int, int) {
	blockid := helper.GetBlockIDForOffset(offset)
	blockStartOffset := helper.GetBlockStartOffsetForBlockID(blockid)
	inBlockOffset := int(offset - blockStartOffset)
	inBlockLength := length
	if inBlockLength > (helper.BlockSize - inBlockOffset) {
		inBlockLength = helper.BlockSize - inBlockOffset
	}

	return inBlockOffset, inBlockLength

}

// GetFirstAndLastBlockIDForRW returns first and last block id for read or write
func (helper *FileBlockHelper) GetFirstAndLastBlockIDForRW(offset int64, length int) (int64, int64) {
	first := helper.GetBlockIDForOffset(offset)
	last := helper.GetBlockIDForOffset(offset + int64(length-1))
	if last < first {
		last = first
	}
	return first, last
}
