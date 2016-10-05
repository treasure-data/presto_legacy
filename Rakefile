#
# Treasure Data:
# This script deploy jar packages with a version number that includes commit-hash of git
# instead of -SNAPSHOT so that we can deploy specific commit to the production system. It
# allows us to deploy hotfixes or improvements before stable releases.
#

EXCLUDE_MODULES = %w|
 presto-atop presto-mongodb
 presto-cassandra presto-kafka presto-redis presto-docs
 presto-benchmark presto-benchmark-driver
 presto-example-http presto-base-jdbc
 presto-mysql presto-postgresql presto-hive
 presto-hive-hadoop1 presto-hive-hadoop2
 presto-verifier presto-testing-server-launcher presto-jmx
 presto-hive-cdh4 presto-hive-cdh5 presto-raptor presto-server-rpm|

EXCLUDE_FROM_COMPILE = %w|presto-docs presto-server-rpm|

def presto_modules
  require "rexml/document"
  pom = REXML::Document.new(File.read("pom.xml"))
  modules = []
  REXML::XPath.each(pom, "/project/modules/module"){|m|
    modules << m.text
  }
  modules
end

def active_modules
  presto_modules.keep_if{|m| !EXCLUDE_MODULES.include?(m) }
end

def compile_target_modules
  presto_modules.keep_if{|m| !EXCLUDE_FROM_COMPILE.include?(m) }
end

desc "compile codes"
task "compile" do
  sh "mvn test-compile -pl #{compile_target_modules.join(",")} -DskipTests"
end

desc "run tests"
task "test" do
  sh "mvn -P td -pl #{active_modules.join(",")} test"
end

desc "set a unique version and td-specific settings"
task "update-pom" do
  require "rexml/document"

  # Read the current presto version
  rev = `git rev-parse HEAD`
  pom = REXML::Document.new(File.read("pom.xml"))
  presto_version = REXML::XPath.first(pom, "/project/version")

  # Set (presto-version)-(git revision number:first 7 characters) version to pom.xml files
  version = "#{presto_version.text.gsub("-SNAPSHOT", "")}-#{rev[0...7]}"
  sh "mvn versions:set -DgenerateBackupPoms=false -DnewVersion=#{version} -N"

  # Reload pom.xml
  pom = REXML::Document.new(File.read("pom.xml"))

  # delete unncessary modules from pom.xml
  # EXCLUDE_MODULES.each{|m|
  #    pom.delete_element("/project/modules/module[text()='#{m}']")
  # }

  # Inject extension plugin to deploy artifacts to s3
  extension = <<EOF
    <extensions>
      <extension>
        <groupId>org.springframework.build</groupId>
        <artifactId>aws-maven</artifactId>
        <version>5.0.0.RELEASE</version>
      </extension>
    </extensions>
EOF
  REXML::XPath.first(pom, "/project/build").add_element(REXML::Document.new(extension))

  # Inject build profile for TD (disable version check, set distribution management tag)
  profiles = REXML::XPath.first(pom, "/project/profiles")
  profiles.add_element(REXML::Document.new(File.read("td-profile.xml")))

  # Dump pom.xml
  File.open('pom.xml', 'w'){|f| pom.write(f) }

end

desc "deploy presto"
task "deploy" do
  # Deploy
  # Deploy presto-root
  sh "mvn -s settings.xml deploy -P td -N -DskipTests"
  # Deploy presot modules
  sh "mvn -s settings.xml deploy -P td -pl #{compile_target_modules.join(",")} -DskipTests"
end
