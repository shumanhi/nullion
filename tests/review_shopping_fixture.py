"""Fictional catalog with mandatory decision facts and optional specifications."""


def shopping_fixture():
    data=[('CedarBond', '$9.98 for 8 fl oz ($1.25/fl oz). Best value for larger wood repairs.',
           'Wood and mixed hard surfaces; waterproof. Expands during curing.'),
          ('StoneEpoxy', '$9.88 for 0.85 fl oz ($11.62/fl oz). Strong small repairs.',
           'Wood and mixed hard surfaces; waterproof. Two-part adhesive.'),
          ('QuickEpoxy', '$5.97 for 0.85 fl oz ($7.02/fl oz). Cheapest upfront.',
           'Wood and mixed hard surfaces; water-resistant. Small syringe.')]
    items=[]; sources=[]; verbose=[]
    for i,(name,summary,details) in enumerate(data):
        optional='Optional specification catalog '+str(i)+': '+('Full compatibility and application instructions. '*15)
        items.append({'title':name,'summary':summary,'required_details':[details],
            'extra_details':[optional,'Exact full-cure and seller details are unavailable.'],
            'source_ids':[str(i)],'shared_detail_ids':[]})
        sources.append({'id':str(i),'label':'Listing','url':'https://catalog.example/'+str(i)})
        verbose.append(name+'\n'+summary+'\n'+details+'\n'+optional)
    message={'summary':'The larger bottle is the best value; the syringes suit smaller repairs.',
        'detail_level':'compact','sections':[{'heading':'Compared options','priority':'primary','layout':'list','items':items}],
        'sources':sources,'shared_details':[],'footer':'Comparison covers these three checked listings.'}
    return message,'\n\n'.join(verbose),{'value':7,'records':items,'sources':sources}
