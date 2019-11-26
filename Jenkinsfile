//Jenkinsfile(Declarative Pipeline)
pipeline{
	agent any
	stages{
		stage('Build'){
			steps{
				//checkout scm
				echo "Build URL is ${env.BUILD_URL}"
				bat 'mvn -verison'
				bat 'mvn clean'
				bat 'mvn -B --settings settings.xml compile -DskipTests'
			}
		}
		
		stage('Generate View&Extract Data to file') {
			steps{
				bat 'mvn exec:java -Dexec.mainClass=com.refinitiv.ejvqa.entry.ViewFileGenerationServiceEntry -Dexec.cleanupDaemonThreads=false'
			}
        }
	}
}