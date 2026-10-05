package dev.cerbos.queryplan.exposed

/**
 * One translation: the options it was given, its alias numbering, and the collaborators wired
 * together. Built fresh per `toFilter` call and never shared, so nothing in here is thread-safe
 * and nothing needs to be.
 *
 * Every collaborator holds this object and reaches the others through it at call time, which is
 * what lets them refer to each other without an initialisation order.
 */
internal class Translation(val options: Options) {
    val aliases = AliasAllocator()

    val walker = PlanWalker(this)

    // The scalar side: one mapped column, or an expression over columns, against a constant.
    val leaf = LeafTranslator(this)
    val comparisons = ComparisonTranslator(this)
    val ternary = TernaryTranslator(this)
    val hierarchy = HierarchyTranslator(this)
    val regex = RegexTranslator(this)

    // The relation side: everything that needs a correlated subquery.
    val subqueries = Subqueries(this)
    val collections = CollectionTranslator(this)
    val sizes = SizeTranslator(this)
    val membership = MembershipTranslator(this)

    fun rootScope(): Scope = RootScope(this)
}
