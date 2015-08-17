This directory and the segment directory underneath it contains all the concrete implementations of vectors and the
core query segments that operate on them.
There is a lot of copy/paste inheritance here - most of it unavoidable under the design constraints. Some of it could
be cleaned up a bit more.
Basically every major group of classes (vectors, vector segments, and backing arrays) have an abstract base class that
concentrates (almost) all the common code. The subclasses of the abstract base class is where much repetition is found.

The main design constraints that drive this implementation are:
    * Very high scalability to vectors of 100M to maybe even 1G element range)
    * Wanting compact, fast implementations of large vectors (expected to have segments in the area of 100K to 1M elements)
    * Wanting small files for the large data
    * Wanting highly efficient query terms that can work as fast as possible on large vector segments
    * Support for a memory manager with swap in/out capability for the large data

The key choices that were made are:
    * Vector segments are implemented in two parts: a "metadata" part and a "data" part, where the data part (the large part
      in terms of memory footprint) can be swapped in and out while maintaining the "metadata" part cached in memory.
    * The data part of vector segments is implemented in a backing array class that for the most part just encapsulates
      a primitive array (e.g. long[])
      This accomplishes several things:
        * The backing array can be readily be swapped in and out and produces the most compact file possible (sans
          compression) in most cases
        * The backing array processing (especially in queries) can be extremely fast
        * The array can be passed "by reference", partially overcoming the language limitation and entirely avoiding
          copies of large arrays
    * There are a few problems with the Java language in implementing this approach:
        * primitive types and primitive arrays cannot be used as template parameters in Java generics
        * arrays cannot be passed by reference
        * somewhat limited member visibility control (e.g. no "friends")
        * runtime erasure of parametrized types
        * related to that - inability to infer the runtime type easily (can do some tricks using introspection)
    * Given these limitations, some breaking of common OO rules takes place:
        * the primitive array in the backing array is public to as to allow direct access to it without an array copy
            * we pay by having to carefully conform to an immutability contract on this member
        * any code that cannot use generics due to the use of primitive types must be editor-inherited (in contrast,
          C++ templates basically automate, organize, and hide the process of editor inheritance, but the resulting code
          is basically the same after compilation) - this is the core driver for repeated code
        * while all of the above is generally hidden from the user, the constructors expose this to an extend by
          requiring you to know which type of concrete vector you are constructing (e.g. new LongVector(...)). While
          this can probably be hidden behind some kind of factory, there is little point in that, as in an analytic
          context one is expected to make such concrete decisions in user code.