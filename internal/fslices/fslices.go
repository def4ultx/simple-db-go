package fslices

func Map[S ~[]E, D ~[]T, E any, T any](x S, f func(E) T) D {
	r := make(D, len(x))
	for i := range x {
		r[i] = f(x[i])
	}
	return r
}

func FlatMap[S ~[]E, D ~[]T, E any, T any](x S, f func(E) []T) D {
	r := make(D, 0)
	for i := range x {
		r = append(r, f(x[i])...)
	}
	return r
}

func Filter[S ~[]E, E any](x S, f func(E) bool) S {
	r := make(S, 0)
	for i := range x {
		if f(x[i]) {
			r = append(r, x[i])
		}
	}
	return r
}

func FoldLeft[S ~[]E, E any, T any](x S, i T, f func(T, E) T) T {
	acc := i
	for j := range x {
		acc = f(acc, x[j])
	}
	return acc
}
