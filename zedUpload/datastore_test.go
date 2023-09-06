package zedUpload_test

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const (
	// global parameters
	uploadDir       = "./test/input/"
	uploadFile      = "./test/input/zedupload_test.img"
	uploadFileSmall = "./test/input/zedupload_test_small.img"
)

func ensureFile(filename string, size int64) error {
	if info, err := os.Stat(filename); err != nil || info.Size() != size {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		p := make([]byte, size)
		if _, err := r.Read(p); err != nil {
			return err
		}
		if err := os.WriteFile(filename, p, 0644); err != nil {
			return err
		}
	}
	return nil
}

func setup() error {
	// make sure that our upload files exist
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return err
	}
	if err := ensureFile(uploadFile, 1024*1024); err != nil {
		return fmt.Errorf("error creating upload file %s: %v", uploadFile, err)
	}
	if err := ensureFile(uploadFileSmall, 1024); err != nil {
		return fmt.Errorf("error creating small upload file %s: %v", uploadFileSmall, err)
	}
	return nil
}

func hashFileMd5(filePath string) (string, error) {
	//Initialize variable returnMD5String now in case an error has to be returned
	var returnMD5String string

	//Open the passed argument and check for any error
	file, err := os.Open(filePath)
	if err != nil {
		return returnMD5String, err
	}

	//Tell the program to call the following function when the current function returns
	defer file.Close()

	//Open a new hash interface to write to
	hash := md5.New()

	//Copy the file in the hash interface and check for any error
	if _, err := io.Copy(hash, file); err != nil {
		return returnMD5String, err
	}

	//Get the 16 bytes hash
	hashInBytes := hash.Sum(nil)[:16]

	//Convert the bytes to a string
	returnMD5String = hex.EncodeToString(hashInBytes)

	return returnMD5String, nil

}

func calculateMd5(filename string, chunkSize int64) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()
	dataSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return "", err
	}

	var (
		sumOfSums []byte
		parts     int
	)
	for i := int64(0); i < dataSize; i += chunkSize {
		length := chunkSize
		if i+chunkSize > dataSize {
			length = dataSize - i
		}
		sum, err := md5sum(f, i, length)
		if err != nil {
			return "", err
		}
		sumOfSums = append(sumOfSums, sum...)
		parts++
	}

	var finalSum []byte

	if parts == 1 {
		finalSum = sumOfSums
	} else {
		h := md5.New()
		_, err := h.Write(sumOfSums)
		if err != nil {
			return "", err
		}
		finalSum = h.Sum(nil)
	}
	sumHex := hex.EncodeToString(finalSum)

	if parts > 1 {
		sumHex += "-" + strconv.Itoa(parts)
	}

	return sumHex, nil
}

func md5sum(r io.ReadSeeker, start, length int64) ([]byte, error) {
	_, _ = r.Seek(start, io.SeekStart)
	h := md5.New()
	if _, err := io.CopyN(h, r, length); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

type unstableFileReader struct {
	readSeekCloser     io.ReadSeekCloser
	read               uint64
	sent               uint64
	discarded          uint64
	unstableStartBytes uint64
	unstableStartTime  time.Time
	unstableEndTime    time.Time
	unstableDuration   time.Duration
	dropPercent        int
	rand               *rand.Rand
	logger             func(string, ...interface{})
}

func (u *unstableFileReader) Read(p []byte) (n int, err error) {
	// get the current time
	now := time.Now()

	buf := make([]byte, len(p))
	n, err = u.readSeekCloser.Read(buf)
	if err != nil && err != io.EOF {
		return n, err
	}
	u.read += uint64(n)
	// copy the data if any of the following are true:
	// - we have not yet read the minimum number of bytes
	// - we are not yet at unstable start time
	// - we are past unstable end time, but check that endTime was set
	// - we are within the random setting of percentage to drop

	// it can end during one of:
	// - middle of a read, if the duration is complete before we finished reading
	// - during an EOF, if we finished the reading before duration and are waiting
	if u.read < u.unstableStartBytes || now.Before(u.unstableStartTime) || (!u.unstableEndTime.IsZero() && now.After(u.unstableEndTime)) || u.rand.Intn(100) >= u.dropPercent {
		copy(p, buf)
		u.sent += uint64(n)
	} else {
		// if this is the first discard - i.e. we never set the start or end time for the delay -
		// then set them
		if u.unstableStartTime.IsZero() {
			u.unstableStartTime = now
			u.unstableEndTime = now.Add(u.unstableDuration)
			if u.logger != nil {
				u.logger("Limit starts at: %s", now)
			}
		}
		if now == u.unstableEndTime && u.logger != nil {
			u.logger("Limit ends while still reading at: %s", now)
		}

		// we are at discard stage
		u.discarded += uint64(n)
		n = 0
	}
	if err == io.EOF {
		if now.Before(u.unstableEndTime) {
			if u.logger != nil {
				u.logger("Finished reading before end time, waiting until: %s", u.unstableEndTime)
			}
			time.Sleep(u.unstableEndTime.Sub(now))
			if u.logger != nil {
				u.logger("Limit ends at: %s", time.Now())
			}
		}
		u.logFinal()
	}
	return
}

func (u *unstableFileReader) Seek(offset int64, whence int) (int64, error) {
	return u.readSeekCloser.Seek(offset, whence)
}

func (u *unstableFileReader) Close() error {
	u.logFinal()
	return u.readSeekCloser.Close()
}

func (u *unstableFileReader) logFinal() {
	if u.logger != nil {
		u.logger("Closed (%d bytes read, %d bytes sent, %d bytes discarded)", u.read, u.sent, u.discarded)
	}
}

// newUnstabileFileReader file reader that will drop dropPercent of the data after unstableStartBytes
// is read, for unstableDuration. When the file is completely read, it will close normally.
func newUnstableFileReader(r io.ReadSeekCloser, unstableStartBytes uint64, unstableDuration time.Duration, dropPercent int, logger func(string, ...interface{})) io.ReadSeekCloser {
	return &unstableFileReader{
		readSeekCloser:     r,
		unstableStartBytes: unstableStartBytes,
		unstableDuration:   unstableDuration,
		dropPercent:        dropPercent,
		logger:             logger,
		rand:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}
