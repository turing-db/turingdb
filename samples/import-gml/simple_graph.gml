graph [
    node [
        id 0
        name "remy"
    ]
    node [
        id 1
        name "adam"
    ]
    node [
        id 2
        name "luc"
    ]
    node [
        id 3
        name "maxime"
    ]
    node [
        id 4
        name "martina"
    ]
    node [
        id 15
        name "ghosts"
    ]
    node [
        id 6
        name "bio"
    ]
    node [
        id 7
        name "cooking"
    ]
    node [
        id 8
        name "paddle"
    ]
    node [
        id 9
        name "animals"
    ]
    node [
        id 10
        name "computers"
    ]
    node [
        id 11
        name "eighties"
    ]

    edge [
        source 0
        target 1
        name "Remy -> Adam"
    ]

    edge [
        source 0
        target 15
        name "Remy -> Ghosts"
    ]

    edge [
        source 0
        target 10
        name "Remy -> Computers"
    ]

    edge [
        source 0
        target 11
        name "Remy -> Eighties"
    ]

    edge [
        source 1
        target 0
        name "Adam -> Remy"
    ]

    edge [
        source 1
        target 6
        name "Adam -> Bio"
    ]

    edge [
        source 1
        target 7
        name "Adam -> Cooking"
    ]

    edge [
        source 2
        target 9
        name "Luc -> Animals"
    ]

    edge [
        source 2
        target 10
        name "Luc -> Computers"
    ]

    edge [
        source 3
        target 6
        name "Maxime -> Bio"
    ]

    edge [
        source 3
        target 8
        name "Maxime -> Paddle"
    ]

    edge [
        source 4
        target 7
        name "Martina -> Cooking"
    ]
]
