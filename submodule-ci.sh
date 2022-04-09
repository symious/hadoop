#!/usr/bin/env bash

echo "CI_COMMIT_BEFORE_SHA is ${CI_COMMIT_BEFORE_SHA} and CI_COMMIT_SHA is $CI_COMMIT_SHA"

a=$(git diff --stat --name-only $CI_COMMIT_BEFORE_SHA $CI_COMMIT_SHA)

echo "Diff code in ${a}"

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
     hadoop-hdfs)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs:
  stage: test
  tags:
    - hadoop2
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-hdfs-client)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-client:
  stage: test
  tags:
    - hadoop2
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs-client
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs-client/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-hdfs-httpfs)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-httpfs:
  stage: test
  tags:
    - hadoop2
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs-httpfs
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs-httpfs/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-hdfs-native-client)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-native-client:
  stage: test
  tags:
    - hadoop2
  script:
    - hadoop-hdfs-project/hadoop-hdfs-native-client
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs-native-client/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-hdfs-nfs)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-nfs:
  stage: test
  tags:
    - hadoop2
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs-nfs
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hfs-nfs/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-hdfs-rbf)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-rbf:
  stage: test
  tags:
    - hadoop2
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs-rbf
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs-rbf/target/surefire-reports/TEST-*.xml
EOF
     ;;
     hadoop-hdfs-bkjournal)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-bkjournal:
  stage: test
  tags:
    - hadoop2
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs/src/contrib/bkjournal
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'
  coverage: '/Total.*?([0-9]{1,3})%/'
  artifacts:
    when: always
    reports:
      junit:
        - hadoop-hdfs-project/hadoop-hdfs/src/contrib/bkjournal/target/surefire-reports/TEST-*.xml
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
    - hadoop2
  script:
    - echo empty
EOF
fi
