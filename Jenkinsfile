@Library('jenkins-library@0.28.0') _

gradleVersion='6.8.1'
gradle="/opt/gradle/gradle-${gradleVersion}/bin/gradle"
appName="data-analytics-engine"
ansibleGitLocation="git@gitlab.engati.ops:engati/engati-infrastructure/deployment-ansible.git"

pipeline {
  agent any
  options {
      disableConcurrentBuilds()
      skipDefaultCheckout true
  }

  stages {

    stage('Clean WS and checkout SCM') {
      steps {
        deleteDir()
        checkout scm
        script {
          utils.abortPseudoBuild()
        }
      }
    }

    stage('Clean') {
      steps {
        script {
          sh "${gradle} clean"
        }
      }
    }

    stage('Increment Version') {
      steps {
        script {
          if (versions.isReleaseBuild()) {
            versions.increment('java')
          }
        }
      }
    }

    stage('Build and Publish') {
      steps {
        script {
          version = versions.getVersion('java')
          sh "${gradle} build -PappVersion=${version}"
          artifacts.publishEngati("build/libs/${appName}-${version}.jar", "${appName}/${version}")
          artifacts.publishEngati("config/logback-spring.xml", "${appName}/${version}/config")
          artifacts.publishEngati("config/logback-access-spring.xml", "${appName}/${version}/config")
        }
      }
    }
    stage ('Push to Git and Update Deployment Repo') {
      steps {
        script {
          if (versions.isReleaseBuild()) {
            sshagent(['jenkins-coviam']) {
              sh "git push origin HEAD:$BRANCH_NAME --tags"
            }
            manifest.publish(
              appName.replaceAll('-', '_'),
              ansibleGitLocation,
              'envs/qa',
              versions.getVersion('java')
            )
          }
        }
      }
    }
    stage('Sonarqube') {
      environment {
        scannerHome = tool 'engati-code-scanner'
      }
      steps {
          withSonarQubeEnv('Engati-Sonar') {
            sh "${scannerHome}/bin/sonar-scanner"
           }
      }
    }
  }
  post {
    always {
      script {
        notification.buildStatus("#tech-builds")
      }
      cleanWs()
    }
  }
}
