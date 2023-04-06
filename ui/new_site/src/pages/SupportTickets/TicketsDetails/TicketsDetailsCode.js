import PrismCode from "../../../Components/Common/Prism";

// TICKET DISCRIPATION

const ticketCode =
    `
var app = document.getElementById("app");
var run = (model) => get(model, "users", () =>
    get(model, "posts",
    () => {
        model.users.forEach(user => model.userIdx[user.id] = user);
        app.innerText = '';
        model.posts.forEach(post =>
        app.appendChild(renderPost(post, model.userIdx[post.userId])));
    }));
app.appendChild(Wrapper.generate("button", "Load").click(() => run({
    userIdx: {}
})).element);
`;

const TicketCodeExample = () => (
    <PrismCode
        code={ticketCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export default TicketCodeExample;