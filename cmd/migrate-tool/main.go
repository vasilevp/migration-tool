package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/mongodb/atlas-osb/pkg/mongodbrealm"
	"github.com/sirupsen/logrus"
)

type Args struct {
	PublicKey  string `arg:"required"`
	PrivateKey string `arg:"required"`
	ProjectID  string `arg:"required"`
}

var args Args

func main() {
	arg.MustParse(&args)

	ctx := context.Background()

	logrus.Info("Authorizing to the Realm API")

	client, err := mongodbrealm.New(
		nil,
		mongodbrealm.SetBaseURL("https://realm.mongodb.com/api/admin/v3.0/"),
		mongodbrealm.SetAPIAuth(ctx, args.PublicKey, args.PrivateKey),
	)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Infof("Getting all apps in project %s", args.ProjectID)

	apps, _, err := client.RealmApps.List(ctx, args.ProjectID, nil)
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

	if len(stateApps) <= 1 {
		logrus.Info("Nothing to do")
		return
	}

	logrus.Info("Getting values from apps")

	valueMap := map[string]map[string]mongodbrealm.RealmValue{}

	for _, a := range stateApps {
		logrus.Infof("Getting values from app %s", a.ID)
		values, _, err := client.RealmValues.List(ctx, args.ProjectID, a.ID, nil)
		if err != nil {
			logrus.Fatal(err)
		}

		logrus.Infof("Got %d values", len(values))

		valueMap[a.ID] = map[string]mongodbrealm.RealmValue{}
		for _, v := range values {
			logrus.Infof("Fetching value %s", v.Name)
			fullValue, _, err := client.RealmValues.Get(ctx, args.ProjectID, a.ID, v.ID)
			if err != nil {
				logrus.Fatal(err)
			}
			valueMap[a.ID][v.Name] = *fullValue
		}
	}

	fname := fmt.Sprintf("data_backup.%d.json", time.Now().UnixNano())

	logrus.Infof("Saving backup to %s", fname)
	data, err := json.MarshalIndent(valueMap, "", "\t")
	if err != nil {
		logrus.Fatal(err)
	}

	err = ioutil.WriteFile(fname, data, 0644)
	if err != nil {
		logrus.Fatal(err)
	}

	target := stateApps[0].ID
	logrus.Infof("Selected %s (%s) as the target state app", target, stateApps[0].ClientAppID)

	for app, values := range valueMap {
		if app == target {
			continue
		}

		logrus.Infof("Copying values from %s to %s", app, target)

		for _, value := range values {
			if dupl, ok := valueMap[target][value.Name]; ok {
				logrus.
					WithField("value_in_duplicate", fmt.Sprintf("https://realm.mongodb.com/groups/%s/apps/%s/values/%s", args.ProjectID, app, value.ID)).
					WithField("value_in_target", fmt.Sprintf("https://realm.mongodb.com/groups/%s/apps/%s/values/%s", args.ProjectID, target, dupl.ID)).
					Fatalf("Value with name %s already exists in app ID %s, please confirm and delete by hand", value.Name, target)
			}

			logrus.Infof("Creating value %s in %s", value.Name, target)
			_, _, err := client.RealmValues.Create(ctx, args.ProjectID, target, &value)
			if err != nil {
				logrus.Fatal(err)
			}

			logrus.Infof("Deleting value %s from %s", value.Name, target)
			_, err = client.RealmValues.Delete(ctx, args.ProjectID, app, value.ID)
			if err != nil {
				logrus.Fatal(err)
			}
		}

		logrus.Infof("Deleting app %s", app)
		_, err := client.RealmApps.Delete(ctx, args.ProjectID, app)
		if err != nil {
			logrus.Fatal(err)
		}
	}

	logrus.Infof("Successfully merged Realm Values into app ID %s", target)
}
