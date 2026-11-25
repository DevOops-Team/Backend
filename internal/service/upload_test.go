package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DevOops-Team/Backend/internal/model"
)

const uploadPrefix = "datasets/a1b2c3d4/v1/"

// uploadFiles is a small upload: what the browser hashes and PUTs.
var uploadFiles = map[string]string{
	"model/gpt2/config.json": `{"model_type":"gpt2"}`,
	"model/gpt2/model.bin":   "weights",
	"data/train.jsonl":       `{"text":"hi"}` + "\n",
	"train/my_train.py":      "print('train')\n",
}

func uploadRequest() model.UploadRequest {
	var req model.UploadRequest
	for _, p := range []string{"model/gpt2/config.json", "model/gpt2/model.bin", "data/train.jsonl", "train/my_train.py"} {
		req.Files = append(req.Files, model.UploadFile{Path: p, Size: int64(len(uploadFiles[p])), SHA256: hexSum(uploadFiles[p])})
	}
	return req
}

// putAll stores the files where the presigned URLs point, as the browser's PUTs would.
func putAll(objs fakeObjects) {
	objs[uploadPrefix+"model/config.json"] = uploadFiles["model/gpt2/config.json"]
	objs[uploadPrefix+"model/model.bin"] = uploadFiles["model/gpt2/model.bin"]
	objs[uploadPrefix+"data/train.jsonl"] = uploadFiles["data/train.jsonl"]
	objs[uploadPrefix+"code/train.py"] = uploadFiles["train/my_train.py"]
}

func TestCreateUploadWritesTheManifestAndSignsOnePutPerFile(t *testing.T) {
	objs := fakeObjects{}
	ticket, err := submitService(t, objs, &submitted{}).CreateUpload(context.Background(), uploadRequest())
	if err != nil {
		t.Fatal(err)
	}
	if ticket.ID != "a1b2c3d4" || len(ticket.Files) != 4 || ticket.ExpiresAt.IsZero() {
		t.Fatalf("ticket = %+v", ticket)
	}
	want := map[string]string{"model/gpt2/config.json": "model/config.json", "train/my_train.py": "code/train.py", "data/train.jsonl": "data/train.jsonl"}
	for _, f := range ticket.Files {
		if dst, ok := want[f.Path]; ok && !strings.HasPrefix(f.URL, "https://s3.example/"+uploadPrefix+dst+"?") {
			t.Errorf("%s → %s, want key %s", f.Path, f.URL, uploadPrefix+dst)
		}
		if f.Headers["x-amz-checksum-sha256"] != hexSum(uploadFiles[f.Path]) {
			t.Errorf("%s: the PUT is not signed with the file's sha256: %v", f.Path, f.Headers)
		}
	}
	var m model.DatasetManifest
	if err := json.Unmarshal([]byte(objs[uploadPrefix+"manifest.json"]), &m); err != nil {
		t.Fatal(err)
	}
	if m.JobID != "a1b2c3d4" || m.Model != "gpt2" || len(m.Files) != 4 || m.DataBytes != int64(len(uploadFiles["data/train.jsonl"])) {
		t.Errorf("manifest = %+v", m)
	}
	for key := range objs {
		if key != uploadPrefix+"manifest.json" {
			t.Errorf("wrote %s before the browser uploaded anything", key)
		}
	}
}

func TestCreateUploadChecksTheListLikeAMultipartSubmission(t *testing.T) {
	cases := map[string]func(*model.UploadRequest){
		"no config.json": func(r *model.UploadRequest) { r.Files = r.Files[1:] },
		"two folders":    func(r *model.UploadRequest) { r.Files[1].Path = "model/other/model.bin" },
		"bad sha256":     func(r *model.UploadRequest) { r.Files[0].SHA256 = "XYZ" },
		"escaping path":  func(r *model.UploadRequest) { r.Files[2].Path = "data/../x.jsonl" },
		"model too big":  func(r *model.UploadRequest) { r.Files[1].Size = 3 << 30 },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			req := uploadRequest()
			edit(&req)
			objs := fakeObjects{}
			_, err := submitService(t, objs, &submitted{}).CreateUpload(context.Background(), req)
			var e *Error
			if !errors.As(err, &e) || (e.Kind != KindInvalid && e.Kind != KindTooLarge) {
				t.Fatalf("err = %v, want a validation error", err)
			}
			if len(objs) != 0 {
				t.Errorf("a rejected upload wrote %v", objs)
			}
		})
	}
}

func uploadSpec() model.SubmitSpec {
	return model.SubmitSpec{GPUs: 2, Hyperparameters: map[string]any{}, Upload: &model.SpecUpload{ID: "a1b2c3d4"}}
}

func TestSubmitWithAnUploadUsesItsIDAndWritesNoDataset(t *testing.T) {
	objs := fakeObjects{}
	sched := &submitted{}
	svc := submitService(t, objs, sched)
	if _, err := svc.CreateUpload(context.Background(), uploadRequest()); err != nil {
		t.Fatal(err)
	}
	putAll(objs)
	manifest := objs[uploadPrefix+"manifest.json"]
	svc.cfg.NewID = func() string { return "ffffffff" } // the job must take the upload's id, not a new one

	created, err := svc.Submit(context.Background(), uploadSpec(), &parts{})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "a1b2c3d4" || sched.js == nil {
		t.Fatalf("created = %+v, submitted = %v", created, sched.js != nil)
	}
	if objs[uploadPrefix+"manifest.json"] != manifest {
		t.Error("the upload manifest was rewritten")
	}
	if objs["code/a1b2c3d4/train.py"] != uploadFiles["train/my_train.py"] {
		t.Error("train.py was not copied to code/<id>/")
	}
	var cfg model.TrainConfig
	if err := json.Unmarshal(created.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Model != "gpt2" || cfg.Dataset.URI != "s3://ckpt/"+uploadPrefix || cfg.Dataset.ManifestSHA256 != hexSum(manifest) {
		t.Errorf("config dataset = %+v model %s", cfg.Dataset, cfg.Model)
	}

	// The same upload again: its inputs must not be overwritten.
	if _, err := svc.Submit(context.Background(), uploadSpec(), &parts{}); !isKind(err, KindConflict) {
		t.Errorf("second submission err = %v, want conflict", err)
	}
}

func TestSubmitWithAnUploadRefusesMissingOrDifferentFiles(t *testing.T) {
	for name, edit := range map[string]func(fakeObjects){
		"not uploaded yet": func(o fakeObjects) { delete(o, uploadPrefix+"model/model.bin") },
		"different bytes":  func(o fakeObjects) { o[uploadPrefix+"data/train.jsonl"] = `{"text":"HI"}` + "\n" },
		"no upload":        func(o fakeObjects) { delete(o, uploadPrefix+"manifest.json") },
	} {
		t.Run(name, func(t *testing.T) {
			objs := fakeObjects{}
			sched := &submitted{}
			svc := submitService(t, objs, sched)
			if _, err := svc.CreateUpload(context.Background(), uploadRequest()); err != nil {
				t.Fatal(err)
			}
			putAll(objs)
			edit(objs)
			if _, err := svc.Submit(context.Background(), uploadSpec(), &parts{}); !isKind(err, KindInvalid) {
				t.Fatalf("err = %v, want invalid", err)
			}
			if sched.js != nil {
				t.Error("submitted a JobSet")
			}
		})
	}
}

func TestSubmitWithAnUploadRejectsFilePartsAndBadIDs(t *testing.T) {
	objs := fakeObjects{}
	svc := submitService(t, objs, &submitted{})
	if _, err := svc.CreateUpload(context.Background(), uploadRequest()); err != nil {
		t.Fatal(err)
	}
	putAll(objs)
	withPart := &parts{{Field: "data", Path: "x.jsonl", Body: strings.NewReader("{}")}}
	if _, err := svc.Submit(context.Background(), uploadSpec(), withPart); !isKind(err, KindInvalid) {
		t.Errorf("file parts with spec.upload: err = %v, want invalid", err)
	}
	for _, id := range []string{"", "../x", "smollm2-wikitext2", "A1B2C3D4"} {
		spec := uploadSpec()
		spec.Upload.ID = id
		if _, err := svc.Submit(context.Background(), spec, &parts{}); !isKind(err, KindInvalid) {
			t.Errorf("upload id %q: err = %v, want invalid", id, err)
		}
	}
}

func isKind(err error, k Kind) bool {
	var e *Error
	return errors.As(err, &e) && e.Kind == k
}

// A multipart file: a 5 MiB + 3 byte model in 5 MiB parts (S3's minimum part size).
const mpPart = 5 << 20

var mpModel = strings.Repeat("w", mpPart) + "end"

func multipartRequest() model.UploadRequest {
	req := uploadRequest()
	req.PartBytes = mpPart
	req.Files[1] = model.UploadFile{Path: "model/gpt2/model.bin", Size: int64(len(mpModel)), SHA256: hexSum(mpModel),
		Parts: []string{hexSum(mpModel[:mpPart]), hexSum(mpModel[mpPart:])}}
	return req
}

// putParts stores the parts where the part URLs point, as the browser's PUTs would.
func putParts(objs fakeObjects, uploadID string, parts ...string) {
	for i, p := range parts {
		objs[fmt.Sprintf("mpu/%s/%d", uploadID, i+1)] = p
	}
}

func multipartID(t *testing.T, objs fakeObjects) string {
	var st uploadState
	if err := json.Unmarshal([]byte(objs[uploadPrefix+"upload.json"]), &st); err != nil {
		t.Fatal(err)
	}
	mp, ok := st.Multipart["model/model.bin"]
	if !ok || st.PartBytes != mpPart || len(mp.Parts) != 2 {
		t.Fatalf("upload.json = %+v", st)
	}
	return mp.UploadID
}

func TestCreateUploadSignsEachPartOfAMultipartFile(t *testing.T) {
	objs := fakeObjects{}
	ticket, err := submitService(t, objs, &submitted{}).CreateUpload(context.Background(), multipartRequest())
	if err != nil {
		t.Fatal(err)
	}
	id := multipartID(t, objs)
	for _, f := range ticket.Files {
		if f.Path != "model/gpt2/model.bin" {
			if f.URL == "" || len(f.Parts) != 0 {
				t.Errorf("%s should be a single PUT: %+v", f.Path, f)
			}
			continue
		}
		if f.URL != "" || len(f.Parts) != 2 {
			t.Fatalf("multipart target = %+v", f)
		}
		for i, p := range f.Parts {
			want := []int64{mpPart, 3}[i]
			if p.Number != int32(i+1) || p.Size != want || !strings.Contains(p.URL, "uploadId="+id) ||
				p.Headers["x-amz-checksum-sha256"] != hexSum(mpModel[int64(i)*mpPart:int64(i)*mpPart+want]) {
				t.Errorf("part %d = %+v", i+1, p)
			}
		}
	}
	var m model.DatasetManifest
	if err := json.Unmarshal([]byte(objs[uploadPrefix+"manifest.json"]), &m); err != nil {
		t.Fatal(err)
	}
	for _, e := range m.Files {
		if e.Name == "model/model.bin" && (e.SHA256 != hexSum(mpModel) || e.SizeBytes != int64(len(mpModel))) {
			t.Errorf("manifest keeps the whole-file sha256 for ckpt-init: %+v", e)
		}
	}
}

func TestCreateUploadChecksTheParts(t *testing.T) {
	for name, edit := range map[string]func(*model.UploadRequest){
		"part size too small": func(r *model.UploadRequest) { r.PartBytes = 1 << 20 },
		"wrong part count":    func(r *model.UploadRequest) { r.Files[1].Parts = r.Files[1].Parts[:1] },
		"bad part sha":        func(r *model.UploadRequest) { r.Files[1].Parts[0] = "nothex" },
		"one part only": func(r *model.UploadRequest) {
			r.Files[0].Parts = []string{r.Files[0].SHA256} // config.json is smaller than one part
		},
	} {
		t.Run(name, func(t *testing.T) {
			req := multipartRequest()
			edit(&req)
			objs := fakeObjects{}
			if _, err := submitService(t, objs, &submitted{}).CreateUpload(context.Background(), req); !isKind(err, KindInvalid) {
				t.Fatalf("err = %v, want invalid", err)
			}
			if len(objs) != 0 {
				t.Errorf("a rejected upload wrote %v", objs)
			}
		})
	}
}

func TestSubmitCompletesTheMultipartFileFromItsCheckedParts(t *testing.T) {
	objs := fakeObjects{}
	sched := &submitted{}
	svc := submitService(t, objs, sched)
	if _, err := svc.CreateUpload(context.Background(), multipartRequest()); err != nil {
		t.Fatal(err)
	}
	id := multipartID(t, objs)
	putAll(objs)
	delete(objs, uploadPrefix+"model/model.bin") // a multipart file exists only once completed

	putParts(objs, id, mpModel[:mpPart]) // one of two parts
	if _, err := svc.Submit(context.Background(), uploadSpec(), &parts{}); !isKind(err, KindInvalid) || sched.js != nil {
		t.Fatalf("with a part missing: err = %v, submitted %v", err, sched.js != nil)
	}
	putParts(objs, id, mpModel[:mpPart], "XYZ") // the last part differs
	if _, err := svc.Submit(context.Background(), uploadSpec(), &parts{}); !isKind(err, KindInvalid) || sched.js != nil {
		t.Fatalf("with a different part: err = %v, submitted %v", err, sched.js != nil)
	}

	putParts(objs, id, mpModel[:mpPart], mpModel[mpPart:])
	created, err := svc.Submit(context.Background(), uploadSpec(), &parts{})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "a1b2c3d4" || objs[uploadPrefix+"model/model.bin"] != mpModel {
		t.Errorf("created %s, completed object %d bytes", created.ID, len(objs[uploadPrefix+"model/model.bin"]))
	}
}

// forbiddenHead is the real bucket for the backend role: HeadObject of a missing key is 403, not 404.
type forbiddenHead struct{ bucketObjects }

func (f forbiddenHead) Head(ctx context.Context, key string) (model.ObjectHead, error) {
	if _, ok := f.fakeObjects[key]; !ok {
		return model.ObjectHead{}, errors.New("operation error S3: HeadObject, https response error StatusCode: 403, api error Forbidden")
	}
	return f.fakeObjects.Head(ctx, key)
}

func TestSubmitCompletesAMultipartFileWithoutReadingTheMissingObject(t *testing.T) {
	objs := fakeObjects{}
	sched := &submitted{}
	svc := submitService(t, objs, sched)
	svc.objects = forbiddenHead{bucketObjects{objs}}
	if _, err := svc.CreateUpload(context.Background(), multipartRequest()); err != nil {
		t.Fatal(err)
	}
	id := multipartID(t, objs)
	putAll(objs)
	delete(objs, uploadPrefix+"model/model.bin")
	putParts(objs, id, mpModel[:mpPart], mpModel[mpPart:])

	created, err := svc.Submit(context.Background(), uploadSpec(), &parts{})
	if err != nil {
		t.Fatalf("submit with 403 on the missing object: %v", err)
	}
	if created.ID != "a1b2c3d4" || objs[uploadPrefix+"model/model.bin"] != mpModel || sched.js == nil {
		t.Errorf("created %s, completed %d bytes, submitted %v", created.ID, len(objs[uploadPrefix+"model/model.bin"]), sched.js != nil)
	}
}
