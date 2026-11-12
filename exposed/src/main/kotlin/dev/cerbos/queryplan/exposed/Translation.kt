package dev.cerbos.queryplan.exposed

/** One translation: its options, alias numbering and collaborators. Built per `toFilter` call, never shared. */
internal class Translation(val options: Options) {
    val aliases = AliasAllocator()
    val hopGuards = HopGuards(this)

    val walker = PlanWalker(this)

    // The scalar side: one mapped column, or an expression over columns, against a constant.
    val leaf = LeafTranslator(this)
    val comparisons = ComparisonTranslator(this)
    val ternary = TernaryTranslator(this)
    val hierarchy = HierarchyTranslator(this)
    val regex = RegexTranslator(this)
    val presence = PresenceTranslator(this)

    // The relation side: everything that needs a correlated subquery.
    val subqueries = Subqueries(this)
    val collections = CollectionTranslator(this)
    val sizes = SizeTranslator(this)
    val membership = MembershipTranslator(this)

    fun rootScope(): Scope = RootScope(this)
}
