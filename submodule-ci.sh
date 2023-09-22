#!/usr/bin/env bash
export COMMIT_BEFORE_SHA="$(git rev-parse HEAD~1)"
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
    - hadoop2
    - k8s
    - $RUNNER"

COMMON_SCRIPTS="- export _JAVA_OPTIONS=\"-Djava.net.preferIPv4Stack=true\"
    - mvn test
    - cat target/site/jacoco/index.html | grep -o 'Total[^%]*%'"

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
  $TAGS
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs
    $COMMON_SCRIPTS
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
EOF
     ;;
     hadoop-hdfs-native-client)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-native-client:
  stage: test
  $TAGS
  script:
    - hadoop-hdfs-project/hadoop-hdfs-native-client
    $COMMON_SCRIPTS
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
  $TAGS
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs-nfs
    $COMMON_SCRIPTS
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
EOF
     ;;
     hadoop-hdfs-bkjournal)
     EMPTY=false
     cat <<EOF >> "${CI_CONFIG_FILE}"
hadoop-hdfs-bkjournal:
  stage: test
  $TAGS
  script:
    - cd hadoop-hdfs-project/hadoop-hdfs/src/contrib/bkjournal
    $COMMON_SCRIPTS
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
  $TAGS
  script:
    - echo empty
EOF
fi
