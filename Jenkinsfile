//Jenkinsfile(Declarative Pipeline)
pipeline{
	agent any
	stages{
		stage('Build'){
			steps{
				checkout scm
				bat 'mvn -verison'
				bat 'mvn -B --settings settings.xml compile -DskipTests | grep --line-buffered -v "Download"'
			}
		}
		
		stage('Generate View&Extract Data to file') {
			steps{
				bat 'mvn exec:java -Dexec.mainClass=com.refinitiv.ejvqa.entry.ViewFileGenerationServiceEntry | grep --line-buffered -v "Download"'
			}
        }
	}
}