package item

import (
	"bytes"
	"encoding/binary"
)

// 对页的索引
type KeyIndexItem struct {
	PageId int
}

func (self *KeyIndexItem) Marshal() []byte {
	length := 4
	buffer := make([]byte, 0, length)
	buff := bytes.NewBuffer(buffer)
	binary.Write(buff, binary.BigEndian, uint32(self.PageId))
	return buff.Bytes()
}

func (self *KeyIndexItem) Unmarshal(b []byte) error {
	buff := bytes.NewReader(b)
	var pageId uint32
	binary.Read(buff, binary.BigEndian, &pageId)
	self.PageId = int(pageId)
	return nil
}

func (self *KeyIndexItem) GetData() interface{} {
	return self.PageId
}
