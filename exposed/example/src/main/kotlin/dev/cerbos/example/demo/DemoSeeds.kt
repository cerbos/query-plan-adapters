package dev.cerbos.example.demo

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.nio.file.Path

/**
 * `demo/seeds.json`, parsed. The rows every example application persists and the principals every
 * one of them plans for, read from the shared corpus rather than restated here — a copy per
 * example would be one more thing to update when the domain gains a row, and the roles half of a
 * principal exists nowhere else at all.
 *
 * Corpus files are repository-controlled and structurally checked by `demo/scripts/validate-demo.sh`
 * before this program ever runs; this is not untrusted input.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class DemoSeeds(
    val principals: List<DemoPrincipal>,
    val applicationFilter: ApplicationFilter,
    val documents: List<Document>,
) {

    /** A demo principal: an id, and the roles the policy's rules are keyed on. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class DemoPrincipal(val id: String, val roles: List<String>)

    /**
     * `archived == false AND region == 'emea'` — the application's own predicate, never expressed
     * in policy. It lives in the corpus so `demo/scripts/validate-demo.sh` can recompute usage
     * shape 5 from it and prove that shape discriminates: applying this predicate alone, or the
     * adapter's filter alone, must both give the wrong answer.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class ApplicationFilter(val archived: Boolean, val region: String)

    /** One seed row. `public` is a Kotlin keyword, hence the rename. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Document(
        val id: String,
        val ownerId: String,
        @param:JsonProperty("public") val isPublic: Boolean,
        val region: String,
        val archived: Boolean,
    )

    /** The named principal, or a failure naming the corpus — never a silently anonymous plan. */
    fun principal(id: String): DemoPrincipal =
        principals.firstOrNull { it.id == id }
            ?: throw IllegalArgumentException("demo/seeds.json declares no principal '$id'")

    companion object {
        fun read(seedsFile: Path): DemoSeeds =
            ObjectMapper().registerKotlinModule().readValue(seedsFile.toFile(), DemoSeeds::class.java)
    }
}
