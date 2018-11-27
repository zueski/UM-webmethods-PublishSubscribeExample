LOCALCP=target/um-pub-sub-example-0.1.0-SNAPSHOT.jar

for dd in lib/*.jar; do
	LOCALCP=${LOCALCP}:${dd}
done

java -classpath "${LOCALCP}" com.amway.integration.um.example.Publisher $*
