package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import static edu.rice.pcdp.PCDP.finish;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 * <p>
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    private final static int GLOBAL_MAX_PRIMES = 10;

    /**
     * {@inheritDoc}
     * <p>
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor sieveActor = new SieveActorActor();

        finish(() -> {
            for (int i = 3; i <= limit; i += 2) { // skipping even numbers
                sieveActor.send(i);
            }
            sieveActor.send(0); // signal completion
        });

        int numPrimes = 1; // accounting for 2, the only even prime
        SieveActorActor actor = sieveActor;
        while (actor != null) {
            numPrimes += actor.numLocalPrimes();
            actor = actor.nextActor;
        }
        return numPrimes;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        private SieveActorActor nextActor;
        private final int[] localPrimes;
        private int numLocalPrimes;
        public SieveActorActor() {
            this.localPrimes = new int[GLOBAL_MAX_PRIMES];
            this.numLocalPrimes = 0;
        }

        @Override
        public void process(final Object msg) {
            final int candidate = (Integer) msg;
            if (candidate <= 0) { // termination signal
                if (nextActor != null) {
                    nextActor.send(msg);
                }
                return;
            }

            // Check against local primes
            boolean isPrime = true;
            for (int i = 0; i < numLocalPrimes; i++) {
                if (candidate % localPrimes[i] == 0) {
                    isPrime = false;
                    break;
                }
            }

            if (isPrime) {
                if (numLocalPrimes < localPrimes.length) { // Store locally if there's capacity
                    localPrimes[numLocalPrimes] = candidate;
                    numLocalPrimes++;
                } else { // Pass to the next actor if the current is full
                    if (nextActor == null) nextActor = new SieveActorActor();
                    nextActor.send(candidate);
                }
            }
        }

        public int numLocalPrimes() {
            return numLocalPrimes;
        }
    }
}
