package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"github.com/azure/azure-sdk-for-go/storage"
	"path/filepath"
)


var (
	accountName	string
	accountKey	string
	client		storage.BlobStorageClient
)

func init() {
	accountName = getEnvVarOrExit("ABS_ACCOUNT_NAME")
	accountKey = getEnvVarOrExit("ABS_ACCOUNT_KEY")
	basicClient, err := storage.NewBasicClient(accountName, accountKey)

	onErrorFail(err, "Failed to create client")
	client = basicClient.GetBlobService()
}

func main() {
	dirs := os.Args[1:]
	filepaths := getFiles(dirs)

	if len(filepaths) > 0 {
		syncFiles(filepaths)
	}
}

func getFiles(dirs []string) []string {
	var paths []string
	for _, aDir := range dirs {
		fileInfo, _ := ioutil.ReadDir(aDir)
		for _, file := range fileInfo {
			paths = append(paths, filepath.Join(aDir, file.Name()))
		}
	}
	return paths
}

func syncFiles(files []string) {
	container := createContainer("humane")

	for _, file := range files {
		fmt.Printf("Creating blob for %s\n", file)
		baseFilename := filepath.Base(file)
		blobOptions := &storage.PutBlobOptions{}
		blob := container.GetBlobReference(baseFilename)

		filehandle := openFileOrFail(file)

		err := blob.CreateBlockBlobFromReader(filehandle, blobOptions)
		onErrorFail(err, fmt.Sprintf("Failed to upload %s", file))
	}
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
