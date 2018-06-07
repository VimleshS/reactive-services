## A prototype for reactive Service based on the [talk](https://github.com/wmr513/reactive) given by Mark Rickards at [GIDS18](http://www.developermarch.com/developersummit/presentations.html#devops17)

### This implementaiton is for self-replicating service depending upon message in a kafka cluster

### Component involved in this pattern
1. Consumer
2. Flow monitor
3. Producer
4. Kakfa cluster

### Way to test this setup.
1. Set up a multi-broker cluster, having 3 partition and a replication factor of 3 as specified in [kafka documentation](https://kafka.apache.org/quickstart#quickstart_multibroker)
2. Start cluster
3. Update `kconfig/conf.json` as per you configuration.
4. Navigate to `reactive-services` directory.
5. Start consumer `./consumer.sh`
6. Start flow monitor `./flow-monitor.sh`
7. Start producer `./producer.sh` to apply load on cluster of you choice to see this pattern in action.


### Improvement
I would like to replace `offsetLagFromKafkaConsumerGrp` routine in `flow-monitor/main.go` with a better logic. But wellcome to improve this prototype by any means. Please feel free to suggest a change.