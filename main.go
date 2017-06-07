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
	file, err := os.Open(filePath)
	if err != nil {
		return returnMd5String, err
	}
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
		blobOptions := &storage.PutBlobOptions{}
		blob := container.GetBlobReference(baseFilename)

		filehandle := openFileOrFail(file)
		defer filehandle.Close()

		err := blob.CreateBlockBlobFromReader(filehandle, blobOptions)
		onErrorFail(err, fmt.Sprintf("Failed to upload %s", file))
	}
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
