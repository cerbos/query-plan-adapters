package dev.cerbos.queryplan.elasticsearch;

import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Single source of truth for the Elasticsearch container image used by this adapter, read from
 * {@code elasticsearch-java/ELASTICSEARCH_IMAGE}.
 *
 * <p><b>Why one reference.</b> {@link ElasticsearchSurfaceTest} and
 * {@link ElasticsearchAdversarialConformanceTest} both start a server, and the adversarial suite
 * is a differential against the PDP. If the two suites run different Elasticsearch builds, the
 * store facts the surface suite measures and the oracle comparison stop describing the same
 * engine, and a version-sensitive divergence surfaces in one suite only.
 *
 * <p><b>Why a file rather than a constant here.</b> {@code example/run.sh} starts a server too, and
 * a shell script cannot read a Java constant. The same argument that put the PostgreSQL pin in
 * {@code pgx/POSTGRES_IMAGE}: {@code conformance/scripts/validate-corpus.sh} holds one digest per
 * tag and nothing holds two tags equal, so a second spelling of the reference could be left behind
 * on an older server and stay green.
 *
 * <p><b>Why the digest.</b> A tag is mutable — {@code 8.15.3} can be re-pushed — so a tag-only pin
 * records an intent, not a build. {@code conformance/scripts/validate-corpus.sh} asserts every
 * service image reference in the repository carries a tag <em>and</em> a digest.
 *
 * <p><b>Two Testcontainers consequences of pinning by digest</b>, both deliberate:
 *
 * <ul>
 *   <li>{@link DockerImageName} puts everything before {@code @sha256:} into the repository, so a
 *       {@code repo:tag@digest} reference does not compare equal to the module's expected
 *       {@code repo} and has to declare {@link DockerImageName#asCompatibleSubstituteFor(String)}
 *       explicitly. The pulled reference is unaffected — Docker resolves {@code repo:tag@digest}
 *       to the digest.
 *   <li>{@code ElasticsearchContainer} derives {@code isAtLeastMajorVersion8} from the version
 *       part of the reference, which is the digest once pinned, so it takes its pre-8 branch: it
 *       does not set {@code ELASTIC_PASSWORD} or a CA cert path. Both suites disable security
 *       outright ({@code xpack.security.enabled=false}) and talk plain HTTP, so neither is read.
 *       A suite that wants TLS or authentication has to configure it itself rather than inherit it
 *       from the tag.
 * </ul>
 */
final class ElasticsearchTestImage {

    private static final String REPOSITORY = "docker.elastic.co/elasticsearch/elasticsearch";

    static final DockerImageName IMAGE =
            DockerImageName.parse(reference()).asCompatibleSubstituteFor(REPOSITORY);

    private ElasticsearchTestImage() {}

    private static String reference() {
        Path pinFile = Path.of(System.getProperty("user.dir"), "ELASTICSEARCH_IMAGE");
        try {
            String pinned = Files.readString(pinFile).strip();
            if (!pinned.startsWith(REPOSITORY + ":")) {
                throw new ExceptionInInitializerError(
                        pinFile + " must pin " + REPOSITORY + ", got: " + pinned);
            }
            return pinned;
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Unable to read the pinned Elasticsearch image from " + pinFile + ": " + e);
        }
    }
}
