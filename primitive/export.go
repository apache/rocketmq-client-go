package primitive

type Option func(Options)

type Options interface {
	Apply(Option)
}
