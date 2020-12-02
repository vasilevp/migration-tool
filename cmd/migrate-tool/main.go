package main

import (
	"context"

	"github.com/alexflint/go-arg"
	"github.com/mongodb/atlas-osb/pkg/mongodbrealm"
	"github.com/sirupsen/logrus"
)

type Args struct {
	PublicKey  string `arg:"required"`
	PrivateKey string `arg:"required"`
	ProjectID  string `arg:"required"`
	DryRun     bool
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

	target := stateApps[0].ID
	logrus.Infof("Selected %s (%s) as the target state app", target, stateApps[0].ClientAppID)

	logrus.Info("Getting values from apps")

	totalValues := 0

	for _, a := range stateApps {
		if a.ID == target {
			continue
		}

		logrus.Infof("Counting values in app %s", a.ID)
		values, _, err := client.RealmValues.List(ctx, args.ProjectID, a.ID, nil)
		if err != nil {
			logrus.Fatal(err)
		}

		totalValues += len(values)
	}

	processedValues := 0

	for _, a := range stateApps {
		if a.ID == target {
			continue
		}

		logrus.Infof("Getting values from app %s", a.ID)
		values, _, err := client.RealmValues.List(ctx, args.ProjectID, a.ID, nil)
		if err != nil {
			logrus.Fatal(err)
		}

		logrus.Infof("Got %d values", len(values))

		logrus.Infof("Copying values from %s to %s", a.ID, target)

		for _, v := range values {
			logrus.Infof("Fetching value %s", v.Name)
			fullValue, _, err := client.RealmValues.Get(ctx, args.ProjectID, a.ID, v.ID)
			if err != nil {
				logrus.Fatal(err)
			}

			if args.DryRun {
				logrus.Infof("(DRY RUN) Creating value %s in %s", fullValue.Name, target)
				logrus.Infof("(DRY RUN) Deleting value %s from %s", fullValue.Name, a.ID)
			} else {
				logrus.Infof("Creating value %s in %s", fullValue.Name, target)
				_, _, err = client.RealmValues.Create(ctx, args.ProjectID, target, fullValue)
				if err != nil {
					logrus.Fatal(err)
				}

				logrus.Infof("Deleting value %s from %s", fullValue.Name, a.ID)
				_, err = client.RealmValues.Delete(ctx, args.ProjectID, a.ID, fullValue.ID)
				if err != nil {
					logrus.Fatal(err)
				}
			}

			processedValues++

			logrus.Infof("Processed %d/%d values", processedValues, totalValues)
		}

		if args.DryRun {
			logrus.Infof("(DRY RUN) Deleting app %s", a.ID)
		} else {
			logrus.Infof("Deleting app %s", a.ID)
			_, err = client.RealmApps.Delete(ctx, args.ProjectID, a.ID)
			if err != nil {
				logrus.Fatal(err)
			}
		}
	}

	logrus.Infof("Successfully merged %d Realm Values into app ID %s", processedValues, target)
}
