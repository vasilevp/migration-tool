package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"net/url"
	"path"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/alexflint/go-arg"
	"github.com/mongodb/atlas-osb/pkg/broker/dynamicplans"
	"github.com/mongodb/atlas-osb/pkg/mongodbrealm"
	"github.com/pivotal-cf/brokerapi/domain"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type Args struct {
	PublicKey      string `arg:"required"`
	PrivateKey     string `arg:"required"`
	OrgID          string `arg:"required"`
	StateProjectID string `arg:"required"`
}

var args Args

type InstanceData struct {
	Name         string
	DashboardURL string
	Template     string
}

func readData() (map[string]InstanceData, error) {
	backupFile := "backup/data_backup.1606367704134048000.json"
	originalApp := "5f3ddc7b77e6a4a4473d9a9b"

	logrus.Infof("Reading backup data from %s", backupFile)

	data, err := ioutil.ReadFile(backupFile)
	if err != nil {
		return nil, err
	}

	mapping := map[string]map[string]mongodbrealm.RealmValue{}

	err = json.Unmarshal(data, &mapping)

	result := map[string]InstanceData{}

	for app, values := range mapping {
		if app == originalApp {
			continue
		}

		for uuid, _ := range values {
			filename := path.Join("backup/service_instances", uuid+".json")
			contents, err := ioutil.ReadFile(filename)
			if err != nil {
				logrus.Fatal(err)
			}

			contentsParsed := map[string]interface{}{}

			err = json.Unmarshal(contents, &contentsParsed)
			if err != nil {
				logrus.Fatal(err)
			}

			if errorCode, ok := contentsParsed["error_code"]; ok {
				logrus.Errorf("Instance query returned error: %q, skipping!", errorCode)
				continue
			}

			entity, ok := contentsParsed["entity"].(map[string]interface{})
			if !ok {
				logrus.Errorf("Cannot find 'entity' in %s, skipping", filename)
				continue
			}

			planUUID := entity["service_plan_guid"].(string)

			planFilename := path.Join("backup/service_plans", planUUID+".json")
			planContents, err := ioutil.ReadFile(planFilename)
			if err != nil {
				logrus.Fatal(err)
			}

			planContentsParsed := map[string]interface{}{}

			err = json.Unmarshal(planContents, &planContentsParsed)
			if err != nil {
				logrus.Fatal(err)
			}

			planEntity := planContentsParsed["entity"].(map[string]interface{})
			extra := planEntity["extra"]

			planExtra := map[string]interface{}{}
			err = json.Unmarshal([]byte(extra.(string)), &planExtra)
			if err != nil {
				logrus.Fatal(err)
			}

			t, err := template.
				New("").
				Funcs(sprig.TxtFuncMap()).
				Funcs(template.FuncMap{
					"keyByAlias": func(interface{}, string) string { return "" },
				}).
				Parse(planExtra["template"].(string))

			result[uuid] = InstanceData{
				Name:         entity["name"].(string),
				DashboardURL: entity["dashboard_url"].(string),
				Template:     planExtra["template"].(string),
			}
		}
	}

	return id, err
}

func parsePlan(file string) (*template.Template, error) {
	logrus.Infof("Reading template from %s", file)

	text, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read file")
	}

	return t, err
}

func encodePlan(v dynamicplans.Plan) (string, error) {
	b := new(bytes.Buffer)
	b64 := base64.NewEncoder(base64.StdEncoding, b)
	err := json.NewEncoder(b64).Encode(v)
	if err != nil {
		return "", errors.Wrap(err, "cannot marshal plan")
	}

	err = b64.Close()

	return b.String(), errors.Wrap(err, "cannot finalize base64")
}

func main() {
	arg.MustParse(&args)
	ctx := context.Background()

	data, err := readData()
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Info("Authorizing to the Realm API")

	client, err := mongodbrealm.New(
		nil,
		mongodbrealm.SetBaseURL("https://realm.mongodb.com/api/admin/v3.0/"),
		mongodbrealm.SetAPIAuth(ctx, args.PublicKey, args.PrivateKey),
	)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Infof("Getting all apps in project %s", args.StateProjectID)

	apps, _, err := client.RealmApps.List(ctx, args.StateProjectID, nil)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Infof("Found %d apps", len(apps))

	stateApps := []mongodbrealm.RealmApp{}

	logrus.Info("Filtering for 'broker-state' apps")

	for _, v := range apps {
		if v.Name == "broker-state" {
			logrus.Infof("%s (%s)", v.ID, v.ClientAppID)
			stateApps = append(stateApps, v)
		}
	}

	logrus.Infof("Found %d state apps", len(stateApps))

	logrus.Info("Getting values from apps")

	valueMap := map[string]map[string]mongodbrealm.RealmValue{}

	for _, a := range stateApps {
		logrus.Infof("Getting values from app %s", a.ID)
		values, _, err := client.RealmValues.List(ctx, args.StateProjectID, a.ID, nil)
		if err != nil {
			logrus.Fatal(err)
		}

		logrus.Infof("Got %d values", len(values))

		valueMap[a.ID] = map[string]mongodbrealm.RealmValue{}

		// start from the end hoping this would handle latest values
		for i := len(values) - 1; i >= 0; i-- {
			v := values[i]

			logrus.Infof("Fetching value %s", v.Name)

			fullValue, _, err := client.RealmValues.Get(ctx, args.StateProjectID, a.ID, v.ID)
			if err != nil {
				logrus.Fatal(err)
			}

			if !bytes.Equal(fullValue.Value, []byte("null")) {
				logrus.Infof("Value %s is not null, skipping", fullValue.Name)
				continue
			}

			logrus.Infof("Found null value: %s", fullValue.Name)

			dataValue, ok := data[fullValue.Name]
			if !ok {
				logrus.Errorf("Null value %s not found in backup data! Continuing anyway...", fullValue.Name)
				continue
			}

			// https://cloud.mongodb.com/v2/projectId#clusters/detail/clusterName
			u, err := url.Parse(dataValue.DashboardURL)
			if err != nil {
				logrus.Fatal(err)
			}

			projectID := path.Base(u.Path)

			planContext := dynamicplans.Context{
				"instance_name": dataValue.Name,
				"instance_size": dataValue.ClusterTier,
				"plan_name":     dataValue.PlanName,
				"disk_type":     dataValue.DiskType,
				"project_id":    projectID,
				"org_id":        args.OrgID,
			}

			logrus.Infof("%s parameters: instance name %q, cluster tier %q, project ID %q", fullValue.Name, dataValue.Name, dataValue.ClusterTier, projectID)

			logrus.Info("Preparing template")

			raw := new(bytes.Buffer)
			err = planTemplate.Execute(raw, planContext)
			if err != nil {
				logrus.Fatal(err)
			}

			logrus.Info("Parsing plan")

			dp := dynamicplans.Plan{}
			err = yaml.NewDecoder(raw).Decode(&dp)
			if err != nil {
				logrus.Fatal(err)
			}

			logrus.Info("Encoding plan")

			planEnc, err := encodePlan(dp)
			if err != nil {
				logrus.Fatal(err)
			}

			fixedSpec := domain.GetInstanceDetailsSpec{
				PlanID:       "aosb-cluster-plan-template-restored-plan",
				ServiceID:    "aosb-cluster-service-template",
				DashboardURL: dataValue.DashboardURL,
				Parameters:   planEnc,
			}

			vv, err := json.Marshal(fixedSpec)
			if err != nil {
				logrus.Fatal(err)
			}

			fixedValue := mongodbrealm.RealmValue{
				Name:  fullValue.Name,
				Value: vv,
			}

			logrus.Infof("Creating a copy of %s with fixed data", fullValue.Name)

			_, _, err = client.RealmValues.Create(ctx, args.StateProjectID, a.ID, &fixedValue)
			if err != nil {
				logrus.Fatal(err)
			}

			logrus.Infof("Deleting original value for %s", fullValue.Name)

			_, err = client.RealmValues.Delete(ctx, args.StateProjectID, a.ID, fullValue.ID)
			if err != nil {
				logrus.Fatal(err)
			}
		}
	}
}
