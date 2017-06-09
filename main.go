package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"github.com/azure/azure-sdk-for-go/storage"
	"path/filepath"
	"crypto/md5"
	"io"
	"encoding/hex"
	"encoding/base64"
	"flag"
	"strings"
)


var (
	accountName	string
	accountKey	string
	client		storage.BlobStorageClient
	container	*storage.Container
)

const MaxPutBlobSize = 268435456
const MaxBlockSize = 104857600
const ChunkSize = MaxBlockSize/2

func init() {
	accountName = getEnvVarOrExit("ABS_ACCOUNT_NAME")
	accountKey = getEnvVarOrExit("ABS_ACCOUNT_KEY")
	basicClient, err := storage.NewBasicClient(accountName, accountKey)

	onErrorFail(err, "Failed to create client")
	client = basicClient.GetBlobService()
}

func main() {

	dirsArg := flag.String("dir", ".", "directory(s) to upload")
	containerName := flag.String("container", "default", "container to upload to")
	flag.Parse()

	dirs := strings.Split(*dirsArg, ",")
	container = createContainer(*containerName)

	filepaths := getFiles(dirs)
	existingBlobs := getExistingBlobs()
	if len(filepaths) > 0 {
		syncFiles(filepaths, existingBlobs)
	}
}

func getFiles(dirs []string) []string {
	var paths []string
	for _, aDir := range dirs {
		fileInfo, _ := ioutil.ReadDir(aDir)
		for _, file := range fileInfo {
			path := filepath.Join(aDir, file.Name())
			paths = append(paths, path)
		}
	}
	return paths
}

func getExistingBlobs() map[string]string {
	r, err := container.ListBlobs(storage.ListBlobsParameters{})
	onErrorFail(err, "Could not list existing blobs")
	blobs := make(map[string]string)

	for _, blob := range r.Blobs {
		md5bytes, _ := base64.StdEncoding.DecodeString(blob.Properties.ContentMD5)
		md5str := hex.EncodeToString(md5bytes)
		blobs[blob.Name] = md5str
	}

	return blobs
}

func getFileMd5(filePath string) (string, error) {
	var returnMd5String string

	file := openFileOrFail(filePath)
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return returnMd5String, err
	}
	hashInBytes := hash.Sum(nil)[:16]
	returnMd5String = hex.EncodeToString(hashInBytes)
	return returnMd5String, nil
}

func syncFiles(files []string, blobs map[string]string) {
	for _, file := range files {

		baseFilename := filepath.Base(file)

		if blobExists(file, blobs[baseFilename]) {
			continue
		}

		fmt.Printf("Creating blob for %s\n", file)
		blob := container.GetBlobReference(baseFilename)

		filehandle := openFileOrFail(file)
		defer filehandle.Close()

		filesize := fileSize(filehandle)

		if (filesize <= MaxPutBlobSize) {
			err := blob.CreateBlockBlobFromReader(filehandle, &storage.PutBlobOptions{})
			onErrorFail(err, fmt.Sprintf("Failed to upload %s", file))
		} else {
			fmt.Printf("File is too large (%d), uploading as blocks\n", filesize)
			err := createBlockBlobFromLargeFile(baseFilename, filehandle)
			onErrorFail(err, fmt.Sprintf("Failed to upload %s", file))
		}
	}
}

func createBlockBlobFromLargeFile(name string, file *os.File) error {

	var blocks []storage.Block

	blob := container.GetBlobReference(name)
	blockCount := 0

	fmt.Printf("uploading %s", name)
	for {
		chunk := make([]byte, ChunkSize)
		n, err := file.Read(chunk)

		if err != nil { return err }
		if n == 0 { break }

		block := storage.Block{
			ID:blockName(blockCount),
			Status:storage.BlockStatusLatest,
		}

		fmt.Print(".")
		err = blob.PutBlock(block.ID, chunk[0:n], &storage.PutBlockOptions{})
		if err != nil {
			return err }

		blocks = append(blocks, block)
		if n < ChunkSize { break }
		blockCount++
	}
	blob.PutBlockList(blocks, &storage.PutBlockListOptions{})
	fmt.Println("")
	return nil
}

func blockName(count int) string {
	name := fmt.Sprintf("5%d", count)
	return base64.URLEncoding.EncodeToString([]byte(name))
}

func fileSize(file *os.File) int64 {
	fstat, err := file.Stat()
	onErrorFail(err, "Could not get FileInfo")

	return fstat.Size()
}

func blobExists(file string, blobMd5 string) bool {
	if blobMd5 != "" {
		fileMd5, _ := getFileMd5(file)
		if blobMd5 == fileMd5 {
			return true
		}
	}
	return false
}

func openFileOrFail(f string) *os.File {
	filehandle, err := os.Open(f)
	onErrorFail(err, fmt.Sprintf("Failed to open %s", f))
	return filehandle
}

func createContainer(name string) *storage.Container {
	container := client.GetContainerReference(name)
	options := storage.CreateContainerOptions{
		Access: storage.ContainerAccessTypePrivate,
	}

	_, err := container.CreateIfNotExists(&options)
	onErrorFail(err, "Create container failed")

	return container
}

func onErrorFail(err error, message string) {
	if err != nil {
		fmt.Printf("%s: %s\n", message, err)
		os.Exit(1)
	}
}

func getEnvVarOrExit(varName string) string {
	value := os.Getenv(varName)

	if value == "" {
		fmt.Printf("Missing environment variable %s\n", varName)
		os.Exit(1)
	}

	return value
}
