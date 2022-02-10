// Copyright(c) 2017-2018 Zededa, Inc.
// All rights reserved.

package zedUpload

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/url"

	"time"

	azure "github.com/lf-edge/eve/libs/zedUpload/azureutil"
)

func (ep *AzureTransportMethod) Action(req *DronaRequest) error {
	var err error
	var size int
	var loc string
	var list []string
	var contentLength int64
	var remoteFileMD5 string

	switch req.operation {
	case SyncOpDownload:
		err = ep.processAzureDownload(req)
	case SyncOpUpload:
		loc, err = ep.processAzureUpload(req)
		req.objloc = loc
	case SyncOpDelete:
		err = ep.processAzureBlobDelete(req)
	case SyncOpList:
		list, err, size = ep.processAzureBlobList(req)
		req.imgList = list
	case SyncOpGetObjectMetaData:
		contentLength, remoteFileMD5, err = ep.processAzureBlobMetaData(req)
		req.contentLength = contentLength
		req.remoteFileMD5 = remoteFileMD5
	case SysOpPutPart:
		err = ep.processAzureUploadByChunks(req)
	case SysOpCompleteParts:
		err = ep.processPutBlockListIntoBlob(req)
	case SyncOpGetURI:
		sasURI, err := ep.processGenerateBlobSasURI(req)
		if err == nil {
			req.SasURI = sasURI
		}
	case SysOpDownloadByChunks:
		err = ep.processAzureDownloadByChunks(req)
	default:
		err = fmt.Errorf("Unknown Azure Blob datastore operation")
	}

	req.asize = int64(size)
	if err != nil {
		req.status = fmt.Sprintf("%v", err)
	}
	return err
}

func (ep *AzureTransportMethod) Open() error {
	return nil
}

func (ep *AzureTransportMethod) Close() error {
	return nil
}

// WithSrcIPSelection use the specific ip as source address for this connection
func (ep *AzureTransportMethod) WithSrcIPSelection(localAddr net.IP) error {
	ep.hClient = httpClientSrcIP(localAddr, nil)
	return nil
}

// WithSrcIPAndProxySelection use the specific ip as source address for this
// connection and connect via the provided proxy URL
func (ep *AzureTransportMethod) WithSrcIPAndProxySelection(localAddr net.IP,
	proxy *url.URL) error {
	ep.hClient = httpClientSrcIP(localAddr, proxy)
	return nil
}

// WithSrcIPAndHTTPSCerts append certs for the datastore access
func (ep *AzureTransportMethod) WithSrcIPAndHTTPSCerts(localAddr net.IP, certs [][]byte) error {
	client := httpClientSrcIP(localAddr, nil)
	client, err := httpClientAddCerts(client, certs)
	if err != nil {
		return err
	}
	ep.hClient = client
	return nil
}

// WithSrcIPAndProxyAndHTTPSCerts takes a proxy and proxy certs
func (ep *AzureTransportMethod) WithSrcIPAndProxyAndHTTPSCerts(localAddr net.IP, proxy *url.URL, certs [][]byte) error {
	client := httpClientSrcIP(localAddr, proxy)
	client, err := httpClientAddCerts(client, certs)
	if err != nil {
		return err
	}
	ep.hClient = client
	return nil
}

// bind to specific interface for this connection
func (ep *AzureTransportMethod) WithBindIntf(intf string) error {
	return fmt.Errorf("not supported")
}

func (ep *AzureTransportMethod) WithLogging(onoff bool) error {
	return nil
}

// File upload to Azure Blob Datastore
func (ep *AzureTransportMethod) processAzureUpload(req *DronaRequest) (string, error) {
	file := req.name
	loc, err := azure.UploadAzureBlob(ep.aurl, ep.acName, ep.acKey, ep.container, file, req.objloc, ep.hClient)
	if err != nil {
		return loc, err
	}
	return loc, nil
}

// File download from Azure Blob Datastore
func (ep *AzureTransportMethod) processAzureDownload(req *DronaRequest) error {
	file := req.name
	prgChan := make(azure.NotifChan)
	defer close(prgChan)
	if req.ackback {
		go func(req *DronaRequest, prgNotif azure.NotifChan) {
			ticker := time.NewTicker(StatsUpdateTicker)
			var stats azure.UpdateStats
			var ok bool
			for {
				select {
				case stats, ok = <-prgNotif:
					if !ok {
						return
					}
				case <-ticker.C:
					req.doneParts = stats.DoneParts
					ep.ctx.postSize(req, stats.Size, stats.Asize)
				}
			}
		}(req, prgChan)
	}
	doneParts, err := azure.DownloadAzureBlob(ep.aurl, ep.acName, ep.acKey, ep.container, file, req.objloc, req.sizelimit, ep.hClient, req.doneParts, prgChan)
	req.doneParts = doneParts
	if err != nil {
		return err
	}
	return nil
}

// File delete from Azure Blob Datastore
func (ep *AzureTransportMethod) processAzureBlobDelete(req *DronaRequest) error {
	err := azure.DeleteAzureBlob(ep.aurl, ep.acName, ep.acKey, ep.container, req.name, ep.hClient)
	//log.Printf("Azure Blob delete status: %v", status)
	return err
}

// File list from Azure Blob Datastore
func (ep *AzureTransportMethod) processAzureBlobList(req *DronaRequest) ([]string, error, int) {
	var csize int
	var img []string
	img, err := azure.ListAzureBlob(ep.aurl, ep.acName, ep.acKey, ep.container, ep.hClient)
	if err != nil {
		return img, err, csize
	}
	return img, nil, csize
}

func (ep *AzureTransportMethod) processAzureBlobMetaData(req *DronaRequest) (int64, string, error) {
	size, md5, err := azure.GetAzureBlobMetaData(ep.aurl, ep.acName, ep.acKey, ep.container, req.name, ep.hClient)
	if err != nil {
		return 0, "", err
	}
	return size, md5, nil
}

func (ep *AzureTransportMethod) getContext() *DronaCtx {
	return ep.ctx
}

func (ep *AzureTransportMethod) processAzureUploadByChunks(req *DronaRequest) error {
	return azure.UploadPartByChunk(ep.aurl, ep.acName, ep.acKey, ep.container, req.localName, req.UploadID, ep.hClient, bytes.NewReader(req.Adata))
}

func (ep *AzureTransportMethod) processAzureDownloadByChunks(req *DronaRequest) error {
	readCloser, size, err := azure.DownloadAzureBlobByChunks(ep.aurl, ep.acName, ep.acKey, ep.container, req.name, req.objloc, ep.hClient)
	if err != nil {
		return err
	}
	req.chunkInfoChan = make(chan ChunkData, 1)
	chunkChan := make(chan ChunkData)
	go func(chunkChan chan ChunkData) {
		for chunkData := range chunkChan {
			ep.ctx.postChunk(req, chunkData)
		}
	}(chunkChan)
	return processChunkByChunk(readCloser, size, chunkChan)
}

func (ep *AzureTransportMethod) processGenerateBlobSasURI(req *DronaRequest) (string, error) {
	return azure.GenerateBlobSasURI(ep.aurl, ep.acName, ep.acKey, ep.container, req.localName, ep.hClient, req.Duration)
}

func (ep *AzureTransportMethod) processPutBlockListIntoBlob(req *DronaRequest) error {
	return azure.UploadBlockListToBlob(ep.aurl, ep.acName, ep.acKey, ep.container, req.localName, ep.hClient, req.Blocks)
}

func (ep *AzureTransportMethod) NewRequest(opType SyncOpType, objname, objloc string, sizelimit int64, ackback bool, reply chan *DronaRequest) *DronaRequest {
	dR := &DronaRequest{}
	dR.syncEp = ep
	dR.operation = opType
	dR.name = objname
	dR.ackback = ackback

	// FIXME:...we need this later
	dR.localName = objname
	dR.objloc = objloc

	// limit for this download
	dR.sizelimit = sizelimit
	dR.result = reply

	return dR
}

type AzureTransportMethod struct {
	transport SyncTransportType
	aurl      string
	container string

	//Auth
	authType string
	acName   string
	acKey    string

	failPostTime time.Time
	ctx          *DronaCtx
	hClient      *http.Client
}
