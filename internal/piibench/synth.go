package piibench

import "math/rand/v2"

// Synth exposes the generator's algorithmic, synthetic values one at a
// time (the demo load generator uses it). Every value is generated from
// the seed; none is real personal data. Not safe for concurrent use.
type Synth struct{ g *gen }

// NewSynth returns a deterministic value source.
func NewSynth(seed uint64) *Synth {
	return &Synth{g: &gen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}}
}

// IDCard returns a valid 18-digit resident ID (MOD 11-2, real date and
// province code).
func (s *Synth) IDCard() string { return s.g.idCard() }

// BankCard returns a 16- or 19-digit card number with an allowlisted BIN
// that passes Luhn.
func (s *Synth) BankCard() string { return s.g.bankCard() }

// Phone returns an 11-digit mobile number with an allocated prefix.
func (s *Synth) Phone() string { return s.g.phone() }

// Email returns an address under an example domain.
func (s *Synth) Email() string { return s.g.email() }

// IP returns a synthetic IPv4 address (private or documentation-style ranges).
func (s *Synth) IP() string { return s.g.ip() }

// Name returns a synthetic pinyin user name.
func (s *Synth) Name() string { return s.g.pick(family...) + "." + s.g.pick(given...) }

// IntN returns a uniform int in [0, n).
func (s *Synth) IntN(n int) int { return s.g.r.IntN(n) }

// Hex returns n random hex characters.
func (s *Synth) Hex(n int) string { return s.g.hex(n) }
