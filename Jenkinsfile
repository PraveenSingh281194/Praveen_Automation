pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                echo 'Hello Checkout stage'
                checkout scmGit(branches: [[name: '*/Develop']], extensions: [], userRemoteConfigs: [[credentialsId: '640af1f9-5e02-47f8-9eb7-bee4bf9230a0', url: 'https://github.com/PraveenSingh281194/Praveen_Automation.git']])
            }
        }
        stage('Build') {
            steps {
                echo "build stage"
                git branch: 'Develop', credentialsId: '640af1f9-5e02-47f8-9eb7-bee4bf9230a0', url: 'https://github.com/PraveenSingh281194/Praveen_Automation.git'
                echo "just before bat -------"
                bat 'python Hello_world_Demo.py'
                }
                        }
        stage('Test'){
            steps{
                echo "the job is tested"
            }
            
        }
    }
}
