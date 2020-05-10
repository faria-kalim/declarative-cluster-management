/*
 *
 *  * Copyright © 2017 - 2018 VMware, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 *  * except in compliance with the License. You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the
 *  * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 *  * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.vrg.compiler.monoid;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class MonoidComprehension extends Expr {
    @Nullable private Head head;
    private List<Qualifier> qualifiers = new ArrayList<>();

    MonoidComprehension() {
    }

    public MonoidComprehension(final Head head) {
        this.head = head;
    }

    public MonoidComprehension(final List<Qualifier> qualifiers) {
        this.qualifiers = qualifiers;
    }

    public MonoidComprehension(final Head head, final List<Qualifier> qualifiers) {
        this.head = head;
        this.qualifiers = qualifiers;
    }

    public void addQualifiers(final List<Qualifier> newQualifiers) {
        qualifiers.addAll(newQualifiers);
    }

    public void addQualifier(final Qualifier qualifier) {
        qualifiers.add(qualifier);
    }

    public MonoidComprehension withQualifier(final Qualifier qualifier) {
        final List<Qualifier> newQualifiers = new ArrayList<>(qualifiers);
        newQualifiers.add(qualifier);
        final MonoidComprehension newComprehension = new MonoidComprehension(head);
        newComprehension.addQualifiers(newQualifiers);
        return newComprehension;
    }

    @Nullable
    public Head getHead() {
        return head;
    }

    public List<Qualifier> getQualifiers() {
        return qualifiers;
    }

    @Override
    public String toString() {
        return  String.format("[%s | %s]", head, qualifiers);
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        visitor.visitMonoidComprehension(this, context);
        return null;
    }
}