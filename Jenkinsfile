
try{
    node{
        properties([
            parameters([
                string(name:'DBTag',defaultValue:'mssql',description:'Input Database which you want to connect.')
                string(name:'ip_port',defaultValue:'192.168.99.100:1433',description:'Input IP:Port which you want to connect.')
                string(name:'databaseName1',defaultValue:'data_govcorp',description:'Input the databaseName which you want to create view from')
                string(name:'databaseName2',defaultValue:'scratch',description:'if you don't have access to create view at databaseName1,you can Input other databaseName which you have access')
                string(name:'username',defaultValue:'sa',description:'Input username')
                string(name:'password',defaultValue:'Wuaiyun22019;',description:'Input password')
                string(name:'tableNamePath',defaultValue:'null',
                       description:'If you want to only create view with the specified tableNames,you can input the path where the TableName.xlsx is.
                       If you input null,it will use the project default TableName.xlsx which you can edit on last step.
                       If you input All,it will create view of all the tables in the database.
                       If you input other string,it will be treated as a table name that exists in the database')
                string(name:'schemaPattern',defaultValue:'dbo',description:'Input the schema where you tables are')
                string(name:'extractNum',defaultValue:'1000',description:'Input how many data you want to extract.If you input *,it will extract all')
                string(name:'fileDestPath',defaultValue:'null',description:'Input the Output Dir where you want to store the extract file.If you input null it will use the default dir')
            ])
        ])

        echo "${DBTag}"
        echo "${ip_port}"
        echo "${databaseName1}"
        echo "${databaseName2}"
        echo "${username}"
        echo "${password}"
        echo "${tableNamePath}"
        echo "${schemaPattern}"
        echo "${extractNum}"
        echo "${fileDestPath}"

    //    echo '\u2600 Environment Variables'
    //    sh "env"

        stage('Build') {
            checkout scm
            sh 'mvn -version'
            sh 'mvn -B --settings settings.xml compile -DskipTests | grep --line-buffered -v "Download"'
        }

        stage('Generate View&Extract Data to file') {
            sh 'mvn exec:java -Dexec.mainClass=com.refinitiv.ejvqa.entry.ViewFileGenerationServiceEntry -Dexec.args="${DBTag} ${ip_port} ${databaseName1} ${databaseName2} ${username} ${password} ${tableNamePath} ${schemaPattern} ${extractNum} ${fileDestPath}"'
        }

    }
    currentBuild.result = 'SUCCESS'
} catch (exc) {
     currentBuild.result = 'FAILURE'
     throw exc
 }finally{
     if( currentBuild.result == null){
         currentBuild.result = 'SUCCESS'
     }
     println currentBuild.result

}