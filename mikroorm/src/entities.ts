import {
  Entity,
  PrimaryKey,
  Property,
  ManyToOne,
  ManyToMany,
  Collection,
} from "@mikro-orm/core";

@Entity()
export class User {
  @PrimaryKey()
  id!: string;

  @Property()
  aBool!: boolean;

  @Property()
  aNumber!: number;

  @Property()
  aString!: string;
}

@Entity()
export class NextLevelNestedResource {
  @PrimaryKey()
  id!: string;

  @Property()
  aBool!: boolean;

  @Property()
  aNumber!: number;

  @Property()
  aString!: string;
}

@Entity()
export class NestedResource {
  @PrimaryKey()
  id!: string;

  @Property()
  aBool!: boolean;

  @Property()
  aNumber!: number;

  @Property()
  aString!: string;

  @ManyToOne(() => NextLevelNestedResource)
  nextlevel!: NextLevelNestedResource;
}

@Entity()
export class Tag {
  @PrimaryKey()
  id!: string;

  @Property()
  name!: string;
}

@Entity()
export class Label {
  @PrimaryKey()
  id!: string;

  @Property()
  name!: string;
}

@Entity()
export class SubCategory {
  @PrimaryKey()
  id!: string;

  @Property()
  name!: string;

  @ManyToMany(() => Label)
  labels = new Collection<Label>(this);
}

@Entity()
export class Category {
  @PrimaryKey()
  id!: string;

  @Property()
  name!: string;

  @ManyToMany(() => SubCategory)
  subCategories = new Collection<SubCategory>(this);
}

@Entity()
export class Resource {
  @PrimaryKey()
  id!: string;

  @Property()
  aBool!: boolean;

  @Property()
  aNumber!: number;

  @Property()
  aString!: string;

  @Property({ nullable: true })
  aOptionalString?: string;

  @ManyToOne(() => User)
  createdBy!: User;

  @ManyToMany(() => User)
  ownedBy = new Collection<User>(this);

  @ManyToOne(() => NestedResource)
  nested!: NestedResource;

  @ManyToMany(() => Tag)
  tags = new Collection<Tag>(this);

  @ManyToMany(() => Category)
  categories = new Collection<Category>(this);
}
