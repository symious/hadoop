#!/usr/bin/env bash
if [ -z "$CI_MERGE_REQUEST_DIFF_BASE_SHA" ]; then
  export COMMIT_BEFORE_SHA="$(git rev-parse HEAD~1)"
else
  export COMMIT_BEFORE_SHA=$CI_MERGE_REQUEST_DIFF_BASE_SHA
fi

export COMMIT_SHA="$(git rev-parse HEAD~0)"
echo "COMMIT_BEFORE_SHA is $COMMIT_BEFORE_SHA and COMMIT_SHA is $COMMIT_SHA"

a=$(git diff --stat --name-only $COMMIT_BEFORE_SHA $COMMIT_SHA)

declare -A map

dir=$(cd -P -- "$(dirname -- "${BASH_SOURCE-0}")" >/dev/null && pwd -P)

for i in ${a[@]} 
do
  if [[  "$i" =~ ^[a-z/-]*src* ]]; then
   i=${i%/src*}
   j=${i##*/}
   map[$j]=""
  fi
done

echo "Use runner tag $RUNNER"

TAGS="tags:
    - yarn
    - k8s
    - $RUNNER"

COMMON_SCRIPTS="- export _JAVA_OPTIONS=\"\$_JAVA_OPTIONS -Djava.net.preferIPv4Stack=true\"
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
    - cd target/surefire-reports"

COMMON_SCRIPTS_PARALLEL_TESTS="- export _JAVA_OPTIONS=\"\$_JAVA_OPTIONS -Djava.net.preferIPv4Stack=true\"
    - mvn test -P parallel-tests
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
    - cd target/surefire-reports"

# To make stuff run in MR pipelines
SUFFIX="rules:
    - when: always"

CI_CONFIG_FILE="submodule-ci.yml"

EMPTY=true

cat <<EOF > "${CI_CONFIG_FILE}"
stages:
  - test

EOF

for submodule in ${!map[@]}
do
   case $submodule in
     hadoop-yarn-api)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-api:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-client)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-client:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-common)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-common:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-csi)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-csi:
  stage: test
  $TAGS
  script:
    - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-csi
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-csi/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-registry)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-registry:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-registry
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-registry/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-applicationhistoryservice)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-applicationhistoryservice:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-common)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-common:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-globalpolicygenerator)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-globalpolicygenerator:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-globalpolicygenerator
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-globalpolicygenerator/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-nodemanager)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-nodemanager:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-resourcemanager)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-resourcemanager:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager
    - export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"
    - mvn test -Dtest=CapacitySchedulerConfigGeneratorForTest,TestCapacity\*
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-router)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-router:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-router
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-router/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-sharedcachemanager)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-sharecachemanager:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-sharedcachemanager
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-sharedcachemanager/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-timeline-pluginstorage)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-timeline-pluginstorage:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timeline-pluginstorage
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timeline-pluginstorage/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-timelineservice)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-timelineservice:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-timelineservice-documentstore)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-timelineservice-documentstore:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice-documentstore
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice-documentstore/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-yarn-server-web-proxy)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-web-proxy:
  stage: test
  $TAGS
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
     ;;
     hadoop-hdfs)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs:
  stage: test
  $TAGS
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs
    $COMMON_SCRIPTS_PARALLEL_TESTS
  after_script:
      - |
        [ -e ./hadoop-hdfs-project/hadoop-hdfs/target/surefire-reports/ ] && for FILE in ./hadoop-hdfs-project/hadoop-hdfs/target/surefire-reports/TEST-*.xml; do sed -i -e '/<system-out>.*<\/system-out>/d' \$FILE; sed -i -e '/<system-out>/,/<\/system-out>/d' \$FILE; done
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
      ;;
      hadoop-hdfs-client)
      EMPTY=false
      cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-client:
  stage: test
  $TAGS
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs-client
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs-client/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
      ;;
      hadoop-hdfs-httpfs)
      EMPTY=false
      cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-httpfs:
  stage: test
  $TAGS
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs-httpfs
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs-httpfs/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
      ;;
      hadoop-hdfs-rbf)
      EMPTY=false
      cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-rbf:
  stage: test
  $TAGS
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs-rbf
    $COMMON_SCRIPTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs-rbf/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
      ;;
      hadoop-common)
      EMPTY=false
      cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-common:
  stage: test
  $TAGS
  script:
    - cd hadoop-common-project/hadoop-common
    $COMMON_SCRIPTS_PARALLEL_TESTS
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-common-project/hadoop-common/target/surefire-reports/TEST-*.xml
  $SUFFIX
EOF
      ;;
     *)
     echo default
     ;;
  esac
done

if [[ $EMPTY == true ]]; then
  cat <<EOF >> "${CI_CONFIG_FILE}"
empty:
  stage: test
  $TAGS
  script:
    - echo empty
  $SUFFIX
EOF
fi
