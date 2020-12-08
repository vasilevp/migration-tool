package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"runtime"
	"sync"

	"github.com/alexflint/go-arg"
	"github.com/mongodb/atlas-osb/pkg/broker/dynamicplans"
	"github.com/mongodb/atlas-osb/pkg/mongodbrealm"
	"github.com/pivotal-cf/brokerapi/domain"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Args struct {
	PublicKey      string `arg:"required"`
	PrivateKey     string `arg:"required"`
	OrgID          string `arg:"required"`
	StateProjectID string `arg:"required"`
	InstanceList   string `help:"Path to a json file containing an array of GUIDs of existing CF instances"`
	DryRun         bool
}

var args Args

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

	cleanup := false
	instanceList := []string{}
	instanceMap := map[string]interface{}{}
	if args.InstanceList != "" {
		f, err := ioutil.ReadFile(args.InstanceList)
		if err != nil {
			logrus.WithError(err).Fatalf("Cannot read file %s", args.InstanceList)
		}

		err = json.Unmarshal(f, &instanceList)
		if err != nil {
			logrus.WithError(err).Fatalf("Cannot read instance list from %s", args.InstanceList)
		}

		cleanup = len(instanceList) != 0

		for _, id := range instanceList {
			instanceMap[id] = nil
		}
	}

	if cleanup {
		logrus.Warnf("Cleanup mode enabled - this will delete any records not present in %s", args.InstanceList)
	}

	if args.DryRun {
		logrus.Warn("Dry run enabled - no modifications will be sent to the Realm API")
	}

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

	if len(stateApps) > 1 {
		logrus.Fatalf("Found %d state apps, please run migrate-tool first!", len(stateApps))
	}

	app := stateApps[0]

	logrus.Info("Getting values from app")

	logrus.Infof("Getting values from app %s", app.ID)
	values, _, err := client.RealmValues.List(ctx, args.StateProjectID, app.ID, nil)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Infof("Got %d values", len(values))

	threads := runtime.GOMAXPROCS(0)

	part := len(values) / threads
	if part == 0 {
		part = 1
	}

	wg := sync.WaitGroup{}

	for i := 0; i < len(values); i += part {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

		out:
			for j := i; j < i+part && j < len(values); j++ {
				v := values[j]

				logger := logrus.WithField("name", v.Name).WithField("id", v.ID).WithField("cleanup", cleanup)

				logger.Info("Fetching value")

				fullValue, _, err := client.RealmValues.Get(ctx, args.StateProjectID, app.ID, v.ID)
				if err != nil {
					logger.WithError(err).Error("Cannot get value")
					continue
				}

				if _, ok := instanceMap[v.Name]; !ok && cleanup {
					logger.Warn("Value not found in instance list - REMOVING")

					if !args.DryRun {
						_, err = client.RealmValues.Delete(ctx, args.StateProjectID, app.ID, fullValue.ID)
						if err != nil {
							logger.WithError(err).Error("Cannot delete value")
							continue
						}
					}

					logger.Warn("Stale value removed")
					continue
				}

				parsedValue := domain.GetInstanceDetailsSpec{}

				err = json.Unmarshal(fullValue.Value, &parsedValue)
				if err != nil {
					logger.WithError(err).Error("Cannot unmarshal value")
					continue
				}

				switch p := parsedValue.Parameters.(type) {
				case string:
					logger.Infof("Value %s is in new format, skipping...", v.Name)

				case map[string]interface{}:
					logger.Warnf("Value %s is in old format, converting...", v.Name)

					var data []byte
					if plan, ok := p["plan"]; ok {
						data, err = json.Marshal(plan)
					} else {
						data, err = json.Marshal(p)
					}

					if err != nil {
						logger.WithError(err).Error("Cannot marshal parameters")
						continue
					}

					dp := dynamicplans.Plan{}
					err = json.Unmarshal(data, &dp)
					if err != nil {
						logger.WithError(err).Error("Cannot unmarshal plan from parameters")
						continue
					}

					planEnc, err := encodePlan(dp)
					if err != nil {
						logger.WithError(err).Error("Cannot encode plan")
						continue
					}

					parsedValue.Parameters = planEnc

					vv, err := json.Marshal(parsedValue)
					if err != nil {
						logger.WithError(err).Error("Cannot marshal value")
						continue
					}

					fixedValue := mongodbrealm.RealmValue{
						Name:  fullValue.Name,
						Value: vv,
					}

					logger.Warnf("Deleting original value for %s", fullValue.Name)

					if !args.DryRun {
						_, err = client.RealmValues.Delete(ctx, args.StateProjectID, app.ID, fullValue.ID)
						if err != nil {
							logger.WithError(err).Error("Cannot delete value")
							continue
						}
					}

					logger.Warnf("Creating a copy of %s with fixed data", fullValue.Name)

					if !args.DryRun {
						_, _, err = client.RealmValues.Create(ctx, args.StateProjectID, app.ID, &fixedValue)
						if err != nil {
							logger.WithError(err).WithField("fixedValue", fixedValue).Error("Cannot create value")
							break out
						}
					}

				default:
					logger.Warnf("Unexpected parameters format: expected string or map[string]interface{}, found %T", p)
				}
			}
		}(i)
	}

	wg.Wait()
}
