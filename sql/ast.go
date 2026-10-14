package sql

type Stmt interface{}

type Expr interface{}

type ColDef struct {
	Name    string
	Type    string
	NotNull bool
	PK      bool
	Unique  bool
	Serial  bool
	Default Expr
}

type CreateTable struct {
	Name        string
	Cols        []ColDef
	PK          []string
	IfNotExists bool
}

type DropTable struct {
	Names    []string
	IfExists bool
}

type CreateIndex struct {
	Name        string
	Table       string
	Cols        []string
	Unique      bool
	IfNotExists bool
}

type CreateView struct {
	Name        string
	Sel         *Select
	IfNotExists bool
}

type DropView struct {
	Names    []string
	IfExists bool
}

type DropIndex struct {
	Name     string
	IfExists bool
}

type Insert struct {
	Table     string
	Cols      []string
	Rows      [][]Expr
	Select    *Select
	Returning []SelItem
	Conflict  *OnConflict
}

type OnConflict struct {
	Cols    []string
	Nothing bool
	Sets    []Set
}

type Set struct {
	Col  string
	Expr Expr
}

type Update struct {
	Table     string
	Alias     string
	Sets      []Set
	Where     Expr
	Returning []SelItem
}

type Delete struct {
	Table     string
	Alias     string
	Where     Expr
	Returning []SelItem
}

type SelItem struct {
	Expr  Expr
	Alias string
	Star  bool
	Table string
}

type TableRef struct {
	Name  string
	Alias string
	Sub   *Select
	Join  string
	On    Expr
}

type Order struct {
	Expr Expr
	Desc bool
}

type Select struct {
	Distinct bool
	Items    []SelItem
	From     []TableRef
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	OrderBy  []Order
	Limit    Expr
	Offset   Expr
	Op       string
	All      bool
	Right    *Select
	ForUpdate bool
}

type Begin struct{ Iso string }
type Commit struct{}
type Rollback struct{}
type SetTx struct{ Iso string }
type SetVar struct{ Name, Value string }
type Show struct{ Name string }
type Empty struct{}

type Lit struct{ V interface{} }
type Col struct{ Table, Name string }
type Param struct{ N int }
type Unary struct {
	Op string
	X  Expr
}
type Binary struct {
	Op   string
	L, R Expr
}
type Func struct {
	Name     string
	Args     []Expr
	Star     bool
	Distinct bool
}
type When struct{ Cond, Then Expr }
type Case struct {
	Operand Expr
	Whens   []When
	Else    Expr
}
type Between struct {
	X, Lo, Hi Expr
	Not       bool
}
type In struct {
	X    Expr
	List []Expr
	Sub  *Select
	Not  bool
}
type Exists struct {
	Sub *Select
	Not bool
}
type Subquery struct{ Sel *Select }
type IsNull struct {
	X   Expr
	Not bool
}
type Cast struct {
	X    Expr
	Type string
}
type Like struct {
	X, Pattern Expr
	Not, ILike bool
}
