package iron_hippo_exe

import (
	"fmt"
	"io"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

const ProjectID = "cpb101demo1"

type DataflowTemplatePostBody struct {
	JobName    string `json:"jobName"`
	GcsPath    string `json:"gcsPath"`
	Parameters struct {
		InputTable      string `json:"inputTable"`
		OutputProjectID string `json:"outputProjectId"`
		OutputKind      string `json:"outputKind"`
	} `json:"parameters"`
	Environment struct {
		TempLocation string `json:"tempLocation"`
		Zone         string `json:"zone"`
	} `json:"environment"`
}

func init() {
	http.HandleFunc("/cron/start", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform"),
			Base:   &urlfetch.Transport{Context: ctx},
		},
	}

	res, err := client.Post(fmt.Sprintf("https://dataflow.googleapis.com/v1b3/projects/%s/templates", ProjectID), "application/json", r.Body)
	if err != nil {
		log.Errorf(ctx, "ERROR dataflow: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err = io.Copy(w, res.Body)
	if err != nil {
		log.Errorf(ctx, "ERROR Copy API response: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(res.StatusCode)
}
