/*
 * (c) Copyright 2010 Talis Systems Ltd.
 * All rights reserved.
 * [See end of file]
 */

package org.openjena.riot.inf;

import java.util.ArrayList ;
import java.util.HashMap ;
import java.util.List ;
import java.util.Map ;

import org.openjena.atlas.lib.Sink ;
import org.openjena.atlas.lib.StrUtils ;

import com.hp.hpl.jena.graph.Node ;
import com.hp.hpl.jena.graph.Triple ;
import com.hp.hpl.jena.query.Query ;
import com.hp.hpl.jena.query.QueryExecution ;
import com.hp.hpl.jena.query.QueryExecutionFactory ;
import com.hp.hpl.jena.query.QueryFactory ;
import com.hp.hpl.jena.query.QuerySolution ;
import com.hp.hpl.jena.query.ResultSet ;
import com.hp.hpl.jena.query.Syntax ;
import com.hp.hpl.jena.rdf.model.Model ;
import com.hp.hpl.jena.vocabulary.RDF ;


/** Apply a fixed set of inference rules to a stream of triples.
 *  This is inference on the A-Box (the data) with respect to a fixed T-Box
 *  (the vocabulary, ontology).
 *  <ul>
 *  <li>rdfs:subClassOf (transitive)</li>
 *  <li>rdfs:subPropertyOf (transitive)</li>
 *  <li>rdfs:domain</li>
 *  <li>rdfs:range</li>
 *  </ul>  
 */
public class InferenceExpanderRDFS implements Sink<Triple>
{
    // Calculates hierarchies (subclass, subproperty) from a model.
    // Assumes that model has no metavocabulary (use an inferencer on the model first if necessary).
    
    // Todo:
    //   rdfs:member
    //   list:member ???
    
    // Work in NodeID space?  But Node id caching solves most of the problems.
    
    // Expanded hierarchy:
    // If C < C1 < C2 then C2 is in the list for C 
    private final Sink<Triple> output ;
    private final Map<Node, List<Node>> transClasses ;
    private final Map<Node, List<Node>> transProperties;
    private final Map<Node, List<Node>> domainList ;
    private final Map<Node, List<Node>> rangeList ;  
    
    static final Node rdfType = RDF.type.asNode() ;
    
    public InferenceExpanderRDFS(Sink<Triple> output, Model vocab)
    {
        this.output     = output ;
        transClasses    = new HashMap<Node, List<Node>>() ;
        transProperties = new HashMap<Node, List<Node>>() ;
        domainList      = new HashMap<Node, List<Node>>() ;
        rangeList       = new HashMap<Node, List<Node>>() ;
        
        // Find classes - uses property paths
        exec("SELECT ?x ?y { ?x rdfs:subClassOf+ ?y }", vocab, transClasses) ;
        
        // Find properties
        exec("SELECT ?x ?y { ?x rdfs:subPropertyOf+ ?y }", vocab, transProperties) ;
        
        // Find domain
        exec("SELECT ?x ?y { ?x rdfs:domain ?y }", vocab, domainList) ;
        
        // Find range
        exec("SELECT ?x ?y { ?x rdfs:range ?y }", vocab, rangeList) ;
    }
    
    private static void exec(String qs, Model model, Map<Node, List<Node>> multimap)
    {
        String preamble = StrUtils.strjoinNL("PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
                                             "PREFIX  rdfs:   <http://www.w3.org/2000/01/rdf-schema#>",
                                             "PREFIX  xsd:    <http://www.w3.org/2001/XMLSchema#>",
                                             "PREFIX  owl:    <http://www.w3.org/2002/07/owl#>",
                                             "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>") ;
        Query query = QueryFactory.create(preamble+"\n"+qs, Syntax.syntaxARQ) ;
        QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
        ResultSet rs = qexec.execSelect() ;
        for ( ; rs.hasNext() ; )
        {
            QuerySolution soln= rs.next() ;
            Node x = soln.get("x").asNode() ;
            Node y = soln.get("y").asNode() ;
            if ( ! multimap.containsKey(x) )
                multimap.put(x, new ArrayList<Node>()) ;
            multimap.get(x).add(y) ;
        }
    }
    
    public void send(Triple triple)
    {
        output.send(triple) ;
        Node s = triple.getSubject() ;
        Node p = triple.getPredicate() ;
        Node o = triple.getObject() ;

        subClass(s,p,o) ;
        subProperty(s,p,o) ;

        // domain() and range() also go through subClass processing. 
        domain(s,p,o) ;
        // Beware of literal subjects.
        range(s,p,o) ;
        
    }

    /*
     * [rdfs8:  (?a rdfs:subClassOf ?b), (?b rdfs:subClassOf ?c) -> (?a rdfs:subClassOf ?c)] 
     * [rdfs9:  (?x rdfs:subClassOf ?y), (?a rdf:type ?x) -> (?a rdf:type ?y)] 
     */
    final private void subClass(Node s, Node p, Node o)
    {
        // Does not output (s,p,o) itself.
        if ( p.equals(rdfType) )
        {
            List<Node> x = transClasses.get(o) ;
            if ( x != null )
                for ( Node c : x )
                    output.send(new Triple(s,p,c)) ;
        }
    }

    // Rule extracts from Jena's RDFS rules etc/rdfs.rules 
    
    /*
     * [rdfs5a: (?a rdfs:subPropertyOf ?b), (?b rdfs:subPropertyOf ?c) -> (?a rdfs:subPropertyOf ?c)] 
     * [rdfs6:  (?a ?p ?b), (?p rdfs:subPropertyOf ?q) -> (?a ?q ?b)] 
     */
    private void subProperty(Node s, Node p, Node o)
    {
        List<Node> x = transProperties.get(p) ;
        if ( x != null )
        {
            for ( Node p2 : x )
                output.send(new Triple(s,p2,o)) ;
        }
    }

    /*
     * [rdfs2:  (?p rdfs:domain ?c) -> [(?x rdf:type ?c) <- (?x ?p ?y)] ]
     * [rdfs9:  (?x rdfs:subClassOf ?y), (?a rdf:type ?x) -> (?a rdf:type ?y)]  
     */
    final private void domain(Node s, Node p, Node o)
    {
        List<Node> x = domainList.get(p) ;
        if ( x != null )
        {
            for ( Node c : x )
            {
                output.send(new Triple(s,rdfType,c)) ;
                subClass(s, rdfType, c) ;
            }
        }
    }

    /*
     * [rdfs3:  (?p rdfs:range ?c)  -> [(?y rdf:type ?c) <- (?x ?p ?y)] ]
     * [rdfs9:  (?x rdfs:subClassOf ?y), (?a rdf:type ?x) -> (?a rdf:type ?y)]  
     */ 
    final private void range(Node s, Node p, Node o)
    {
        // Mask out literal subjects
        if ( o.isLiteral() )
            return ;
        // Range
        List<Node> x = rangeList.get(p) ;
        if ( x != null )
        {
            for ( Node c : x )
            {
                output.send(new Triple(o,rdfType,c)) ;
                subClass(o, rdfType, c) ;
            }
        }
    }

    public void flush()
    { output.flush(); }

    public void close()
    { output.close(); }
    
}
/*
 * (c) Copyright 2010 Talis Systems Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */