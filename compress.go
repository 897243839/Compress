package Compress

//源数据块的解压缩文件
import (
	"archive/zip"
	"compress/zlib"
	"fmt"
	"github.com/golang/snappy"
	"github.com/ipfs/go-datastore"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"io"
)

// 压缩算法类型
type CompressorType int

const (
	// 未知压缩算法
	UnknownCompressor CompressorType = iota
	// zlib压缩算法
	ZlibCompressor
	// zip压缩算法
	ZipCompressor
	// lz4压缩算法
	Lz4Compressor
	// zstd压缩算法
	ZstdCompressor
	// snappy压缩算法
	SnappyCompressor
)

var (
	// zlib压缩算法的标识字节
	zlibHeader = []byte{0x78, 0x9c}
	// zip压缩算法的标识字节
	zipHeader = []byte{0x50, 0x4b}
	// lz4压缩算法的标识字节
	lz4Header = []byte{0x04, 0x22, 0x4d, 0x18}
	// zstd压缩算法的标识字节
	zstdHeader = []byte{0x28, 0xb5, 0x2f, 0xfd}
	// snappy压缩算法的标识字节
	snappyHeader = []byte{0xff, 0x06, 0x00, 0x00}
)

// 获取压缩算法类型
func GetCompressorType(compressedData []byte) CompressorType {

	if bytes.HasPrefix(compressedData, zlibHeader) {
		return ZlibCompressor
	} else if bytes.HasPrefix(compressedData, zipHeader) {
		return ZipCompressor
	} else if bytes.HasPrefix(compressedData, lz4Header) {
		return Lz4Compressor
	} else if bytes.HasPrefix(compressedData, zstdHeader) {
		return ZstdCompressor
	} else if bytes.HasPrefix(compressedData, snappyHeader) {
		return SnappyCompressor
	} else {
		return UnknownCompressor
	}
}

// 根据压缩算法类型进行解压缩
func Decompress(compressedData []byte, compressorType CompressorType) []byte {
	switch compressorType {
	case ZlibCompressor:
		return Zlib_decompress(compressedData)
	case ZipCompressor:
		return Zip_decompress(compressedData)
	case Lz4Compressor:
		return Lz4_decompress(compressedData)
	case ZstdCompressor:
		return Zstd_decompress(compressedData)
	case SnappyCompressor:
		return Snappy_decompress(compressedData)
	default:
		return compressedData
	}
}

// 根据压缩算法类型进行解压缩
func Compress(compressedData []byte, compressorType CompressorType) []byte {
	switch compressorType {
	case ZlibCompressor:
		return Zlib_compress(compressedData)
	case ZipCompressor:
		return Zip_compress(compressedData)
	case Lz4Compressor:
		return Lz4_compress(compressedData)
	case ZstdCompressor:
		return Zstd_compress(compressedData)
	case SnappyCompressor:
		return Snappy_compress(compressedData)
	default:
		return compressedData
	}
}

// lz4解压缩
func Lz4_compress(val []byte) (value []byte) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	_, err := writer.Write(val)

	if err != nil {
		return val
	}
	err = writer.Close()
	if err != nil {
		return val
	}
	return buf.Bytes()
}
func Lz4_decompress(data []byte) (value []byte) {
	//---------------------------解压
	b := bytes.NewReader(data)
	var out bytes.Buffer
	r := lz4.NewReader(b)
	_, err := io.Copy(&out, r)
	if err != nil {
		//println("解压错误", err)
		return data
	}

	return out.Bytes()
}

// snappy解压缩
func Snappy_compress(val []byte) []byte {

	//---------------压缩

	var buf bytes.Buffer
	writer := snappy.NewBufferedWriter(&buf)
	_, err := writer.Write(val)

	if err != nil {
		return val
	}
	err = writer.Close()
	if err != nil {
		return val
	}
	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()
}
func Snappy_decompress(data []byte) (value []byte) {
	//---------------------------解压
	b := bytes.NewReader(data)
	var out bytes.Buffer
	r := snappy.NewReader(b)
	_, err := io.Copy(&out, r)
	if err != nil {
		//println("解压错误", err)
		return data
	}
	return out.Bytes()
}

// zip解压缩
func Zip_compress(val []byte) []byte {
	buf := new(bytes.Buffer)
	w := zip.NewWriter(buf)
	//wr, _ := w.CreateHeader(&zip.FileHeader{
	//	Name:   fmt.Sprintf("block"),
	//	Method: zip.Deflate, // avoid Issue 6136 and Issue 6138
	//})
	wr, err := w.Create("block")
	if err != nil {
		return val
	}
	_, err = wr.Write(val)
	if err != nil {
		return val
	}
	err = w.Close()
	if err != nil {
		return val
	}
	return buf.Bytes()
}
func Zip_decompress(data []byte) (value []byte) {
	//---------------------------解压
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		fmt.Println(err)
		return data
	}
	r, _ := zr.File[0].Open()
	defer r.Close()
	var out bytes.Buffer
	_, err = io.Copy(&out, r)
	if err != nil {
		//println("解压错误", err)
		return data
	}
	return out.Bytes()
}

// zlib解压缩
func Zlib_compress(val []byte) []byte {

	//---------------压缩
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	_, err := zw.Write(val)
	if err != nil {
		return val
	}
	err = zw.Close()
	if err != nil {
		return val
	}
	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()

}
func Zlib_decompress(data []byte) (value []byte) {
	//---------------------------解压
	b := bytes.NewReader(data)
	var out bytes.Buffer
	r, err := zlib.NewReader(b)
	if err != nil {
		println("解压错误", err)
		return data
	}
	defer r.Close()
	_, err = io.Copy(&out, r)
	if err != nil {
		//println("解压错误", err)
		return data
	}
	return out.Bytes()

}

// Zstd解压缩
func Zstd_compress(val []byte) (value []byte) {

	var buf bytes.Buffer
	writer, _ := zstd.NewWriter(&buf)
	_, err := writer.Write(val)
	if err != nil {
		return val
	}
	err = writer.Close()
	if err != nil {
		return val
	}

	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()
}
func Zstd_decompress(data []byte) (value []byte) {
	//---------------------------解压
	b := bytes.NewReader(data)
	var out bytes.Buffer
	r, err := zstd.NewReader(b)
	if err != nil {
		return data
	}
	defer r.Close()
	_, err = io.Copy(&out, r)
	if err != nil {
		//println("解压错误", err)
		return data
	}
	return out.Bytes()
}
