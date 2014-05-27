/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.groupcomm.matmul;

import java.util.Formatter;
import java.util.Locale;

/**
 * Abstract base class for {@link Vector} implementations.
 * 
 * The only methods to be implemented by subclasses are get, set and size.
 * 
 * @author Markus Weimer <mweimer@microsoft.com>
 * 
 */
abstract class AbstractVector implements Vector {

    @Override
    public abstract void set(int i, double v);

    @Override
    public abstract double get(int i);

    @Override
    public abstract int size();

    @Override
    public double dot(final Vector that) {
        assert (this.size() == that.size());

        double result = 0.0;
        for (int index = 0; index < this.size(); ++index) {
            result += this.get(index) * that.get(index);
        }
        return result;
    }

    @Override
    public void add(final Vector that) {
        assert (this.size() == that.size());
        for (int index = 0; index < this.size(); ++index) {
            this.set(index, this.get(index) + that.get(index));
        }
    }

    @Override
    public void multAdd(final double factor, final Vector that) {
        assert (this.size() == that.size());
        for (int index = 0; index < this.size(); ++index) {
            this.set(index, this.get(index) + factor * that.get(index));
        }
    }

    @Override
    public void scale(final double factor) {
        for (int index = 0; index < this.size(); ++index) {
            this.set(index, this.get(index) * factor);
        }
    }

    @Override
    public double sum() {
        double result = 0.0;
        for (int i = 0; i < this.size(); ++i) {
            result += this.get(i);
        }
        return result;
    }

    @Override
    public double norm2() {
        double result = 0.0;
        for (int i = 0; i < this.size(); ++i) {
            result += Math.pow(this.get(i), 2.0);
        }
        return Math.sqrt(result);
    }

    @Override
    public void normalize() {
        final double factor = 1.0 / this.norm2();
        this.scale(factor);
    }

    @SuppressWarnings("boxing")
    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder("DenseVector(");
        try (final Formatter formatter = new Formatter(b, Locale.US)) {
            for (int i = 0; i < this.size() - 1; ++i) {
                formatter.format("%1.3f, ", this.get(i));
            }
            formatter.format("%1.3f", this.get(this.size() - 1));
        }
        b.append(')');
        return b.toString();
    }
}
