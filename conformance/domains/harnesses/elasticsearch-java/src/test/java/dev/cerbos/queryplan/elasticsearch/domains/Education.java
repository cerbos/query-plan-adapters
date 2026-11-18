package dev.cerbos.queryplan.elasticsearch.domains;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

import java.util.Set;

class Education implements EsDomain {

    public String name() { return "education"; }

    public String properties() {
        return """
            {"id": {"type":"keyword"}, "studentId": {"type":"keyword"}, "submittedAt": {"type":"date"}, "score": {"type":"double"}, "late": {"type":"boolean"}, "plagiarismScore": {"type":"double"},
             "status": {"type":"keyword"}, "locked": {"type":"boolean"}, "members": {"type":"keyword"},
             "assignment": {"properties": {"dueAt": {"type":"date"}, "maxPoints": {"type":"integer"}, "groupWork": {"type":"boolean"},
               "course": {"properties": {"code": {"type":"keyword"}, "term": {"type":"keyword"},
                 "department": {"properties": {"id": {"type":"keyword"}}},
                 "staff": {"type": "nested", "properties": {"userId": {"type":"keyword"}, "role": {"type":"keyword"}}}}}}}}
            """;
    }

    public Options options() {
        return EsDomain.fromMapping(properties())
                .withCollectionFields(Set.of("members"));
    }
}
