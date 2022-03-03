#!/usr/bin/env bash

a=$(git diff --stat --name-only $CI_COMMIT_BEFORE_SHA $CI_COMMIT_SHA)

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
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-client)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-client:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-common)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-common:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-csi)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-csi:
  stage: test
  tags:
    - yarn
  script:
    - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-csi
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-csi/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-registry)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-registry:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-registry
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-registry/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-applicationhistoryservice)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-applicationhistoryservice:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-common)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-common:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-globalpolicygenerator)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-globalpolicygenerator:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-globalpolicygenerator
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-globalpolicygenerator/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-nodemanager)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-nodemanager:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-resourcemanager)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-resourcemanager:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager
    - mvn test -Dtest=CapacitySchedulerConfigGeneratorForTest,TestCapacity\* 
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-router)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-router:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-router
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-router/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-sharedcachemanager)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-sharecachemanager:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-sharedcachemanager
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-sharedcachemanager/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-timeline-pluginstorage)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-timeline-pluginstorage:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timeline-pluginstorage
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timeline-pluginstorage/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-timelineservice)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-timelineservice:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-timelineservice-documentstore)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-timelineservice-documentstore:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice-documentstore
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-timelineservice-documentstore/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-yarn-server-web-proxy)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-yarn-server-web-proxy:
  stage: test
  tags:
    - yarn
  script:
    - cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/target/surefire-reports/TEST-*.xml
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
  tags:
    - yarn
  script:
    - echo empty
EOF
fi
